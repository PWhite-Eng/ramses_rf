"""RAMSES RF - The Asynchronous Topology Builder Engine."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from ramses_rf.const import I_, SZ_DOMAIN_ID, SZ_UFH_IDX, SZ_ZONE_IDX, Code, DevType
from ramses_rf.enums import TopologyAction
from ramses_rf.messages.core import Message
from ramses_rf.models import TopologyChangedEvent
from ramses_rf.protocol.ramses import CODES_ONLY_FROM_CTL, HVAC_KLASS_BY_VC_PAIR

_LOGGER = logging.getLogger(__name__)


class TopologyBuilder:
    """Centralised engine for heuristic eavesdropping and graph mutation.

    INDUSTRY ARCHITECTURE NOTE: The Rule Engine Pattern
    ---------------------------------------------------
    Because this project relies on reverse-engineered RF protocols, new
    heuristics and manufacturer quirks are discovered frequently. To
    prevent this class from becoming a massive, unmaintainable
    "God Object", we register independent heuristic rules in a list.

    When a message arrives, the engine simply feeds the message to every
    rule in the list. To add a new quirk for a new device, a developer
    simply writes a new method and appends it to `self._rules`. The core
    ingestion logic never needs to be modified.
    """

    def __init__(
        self,
        emit_event_cb: Callable[[TopologyChangedEvent], None],
        enable_eavesdrop: bool = False,
    ) -> None:
        """Initialize the TopologyBuilder.

        :param emit_event_cb: Callback to emit topology events back
            onto the central event bus or directly to the registry.
        :param enable_eavesdrop: If False, heuristic class promotions
            are disabled. Explicit bindings (e.g., 000C) still process.
        """
        self._emit = emit_event_cb
        self._enable_eavesdrop = enable_eavesdrop

        # The active list of heuristic rules. Order does not matter,
        # as each rule independently yields its own isolated events.
        self._rules: list[Callable[[Message], None]] = [
            self._evaluate_evohome_rules,
            self._evaluate_zone_binding_rules,
            self._evaluate_directed_telemetry_rules,
            self._evaluate_ufh_rules,
            self._evaluate_hvac_rules,
            self._evaluate_dhw_opentherm_rules,
            self._evaluate_heating_prefix_rules,
            self._evaluate_appliance_control_sync_rules,
            self._evaluate_eavesdrop_rules,
            self._evaluate_implicit_binding_rules,
            self._evaluate_third_address_broadcast_rules,
        ]

    async def consume(self, msg: Message) -> None:
        """Ingest a message and evaluate it against all registered rules.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        for rule in self._rules:
            try:
                rule(msg)
            except Exception as err:
                # Isolate rule execution. A crash in a new, experimental
                # quirk rule must not bring down the discovery pipeline.
                _LOGGER.error(f"Error evaluating topology rule {rule.__name__}: {err}")

    def _evaluate_evohome_rules(self, msg: Message) -> None:
        """Evaluate rules specific to the Evohome CH/DHW ecosystem.

        Historically, entities intercepted CODES_ONLY_FROM_CTL to
        dynamically promote themselves to Controllers. We now extract
        that logic into this explicit, trackable rule.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        if msg.header.verb == I_ and msg.header.code in CODES_ONLY_FROM_CTL:
            event = TopologyChangedEvent(
                action=TopologyAction.CREATE_CONTROLLER,
                device_id=msg.src.id,
                causation="Rule_Evohome_Controller_Broadcast",
            )
            self._emit(event)

    def _evaluate_zone_binding_rules(self, msg: Message) -> None:
        """Evaluate 000C and heuristic packets to bind actuators to zones.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        # EXPLICIT BINDING: Controllers (01) broadcasting 000C device maps
        if msg.header.code == Code._000C and msg.src.type == "01":
            # Safely extract array from strict L7 data envelope
            payloads = msg.data.get("_array", [msg.data])

            for p in payloads:
                if not isinstance(p, dict):
                    continue

                zone_idx = p.get("zone_idx")
                domain_id = p.get("domain_id")
                device_role = p.get("device_role")
                devices = p.get("devices", [])

                if not devices:
                    continue

                # Prepare the base metadata dict, correctly flagging
                # all types of sensors (e.g., 'sensor', 'dhw_sensor')
                metadata: dict[str, Any] = {}
                if device_role is not None:
                    metadata["is_sensor"] = "sensor" in str(device_role)
                    # Explicit DHW preservation
                    metadata["device_role"] = str(device_role)

                if zone_idx is not None:
                    # Clone metadata to avoid cross-iteration pollution
                    event_meta = dict(metadata)
                    event_meta["zone_idx"] = str(zone_idx)
                    # Bridging quirk: DeviceRegistry expects domain index under "child_id"
                    event_meta["child_id"] = str(zone_idx)
                    for child_id in devices:
                        event = TopologyChangedEvent(
                            action=TopologyAction.BIND_DEVICE,
                            parent_id=msg.src.id,  # The Controller
                            child_id=child_id,  # The Device
                            metadata=event_meta,
                            causation="Rule_000C_Zone_Binding",
                        )
                        self._emit(event)

                elif domain_id is not None:
                    event_meta = dict(metadata)
                    event_meta["domain_id"] = str(domain_id)
                    event_meta["child_id"] = str(domain_id)
                    for child_id in devices:
                        event = TopologyChangedEvent(
                            action=TopologyAction.BIND_DEVICE,
                            parent_id=msg.src.id,  # The Controller
                            child_id=child_id,  # The Device
                            metadata=event_meta,
                            causation="Rule_000C_Domain_Binding",
                        )
                        self._emit(event)

    def _evaluate_directed_telemetry_rules(self, msg: Message) -> None:
        """Evaluate implicit bindings from directed telemetry broadcasts.

        Devices (TRVs, Thermostats, DHW sensors) explicitly declare their
        topological relationships by broadcasting telemetry directly to
        their parent Controller, or via addr3 broadcasts.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        BINDING_CODES = (
            Code._2309,
            Code._3150,
            Code._30C9,
            Code._0008,
            Code._0004,
            Code._1260,
            Code._10A0,
            Code._12B0,
            Code._000A,
            Code._2349,
        )

        # Identify the Controller ID whether it is a directed target or a broadcast addr3 target
        ctl_id = None
        if msg.dst.type == "01":
            ctl_id = msg.dst.id
        elif getattr(msg.addr3, "type", None) == "01":
            ctl_id = msg.addr3.id

        if msg.header.verb == I_ and ctl_id and msg.src.id != ctl_id:
            if msg.header.code not in BINDING_CODES:
                return

            payloads = msg.data.get("_array", [msg.data])

            for p in payloads:
                if not isinstance(p, dict):
                    continue

                zone_idx = p.get(SZ_ZONE_IDX)
                domain_id = p.get(SZ_DOMAIN_ID)

                metadata: dict[str, Any] = {}

                # Explicitly flag actuators vs sensors for the registry
                if msg.header.code in (Code._3150, Code._0008, Code._2309, Code._000A):
                    metadata["device_role"] = "actuator"
                elif msg.header.code in (
                    Code._30C9,
                    Code._1260,
                    Code._10A0,
                    Code._12B0,
                ):
                    metadata["device_role"] = "sensor"
                    metadata["is_sensor"] = "True"

                if zone_idx is not None:
                    metadata["zone_idx"] = str(zone_idx)
                    metadata["child_id"] = str(zone_idx)
                elif domain_id is not None:
                    if domain_id in ("F9", "FA", "FC"):
                        metadata["domain_id"] = str(domain_id)
                        metadata["child_id"] = str(domain_id)
                    else:
                        metadata["zone_idx"] = str(domain_id)
                        metadata["domain_id"] = str(domain_id)
                        metadata["child_id"] = str(domain_id)
                else:
                    continue  # Nothing to bind

                self._emit(
                    TopologyChangedEvent(
                        action=TopologyAction.BIND_DEVICE,
                        parent_id=ctl_id,
                        child_id=msg.src.id,
                        metadata=metadata,
                        causation="Rule_Telemetry_Eavesdrop_Binding",
                    )
                )

    def _evaluate_ufh_rules(self, msg: Message) -> None:
        """Evaluate rules specific to Underfloor Heating (UFH).

        UFCs broadcast their circuit mappings via 000C messages.
        We intercept these to bind the UFC to the Controller and map
        the individual circuits to their corresponding zones.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        # Prefix Guard: Ensure the source is an Underfloor Heating Controller
        if msg.src.type != "02" or msg.header.code != Code._000C:
            return

        # UFC 000C packets are addressed directly to the parent Controller
        ctl_id = msg.dst.id if msg.dst.type == "01" else None

        ufc_id = msg.src.id

        # 1. Bind the UFC to the parent Controller if directed
        if ctl_id and ctl_id != "--:------":
            self._emit(
                TopologyChangedEvent(
                    action=TopologyAction.BIND_DEVICE,
                    parent_id=ctl_id,
                    child_id=ufc_id,
                    metadata={"device_role": "ufc"},
                    causation="Rule_UFH_000C_Binding",
                )
            )

        # 2. Create the Circuit and map it to the Zone
        payloads = msg.data.get("_array", [msg.data])

        for p in payloads:
            if not isinstance(p, dict):
                continue

            ufh_idx = p.get(SZ_UFH_IDX)
            zone_idx = p.get(SZ_ZONE_IDX)

            if ufh_idx is not None:
                self._emit(
                    TopologyChangedEvent(
                        action=TopologyAction.CREATE_CIRCUIT,
                        device_id=ufc_id,
                        metadata={
                            "ufh_idx": str(ufh_idx),
                            "zone_idx": str(zone_idx) if zone_idx else "None",
                            "child_id": str(zone_idx) if zone_idx else "None",
                        },
                        causation="Rule_UFH_000C_Circuit",
                    )
                )

    def _evaluate_hvac_rules(self, msg: Message) -> None:
        """Evaluate rules specific to Ventilation and HVAC.

        HVAC devices share prefixes (e.g., 32: can be a Fan, CO2, etc.).
        Therefore, we promote classes dynamically using the central protocol schema.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        try:
            code_enum = Code(msg.header.code)
        except ValueError:
            return

        # Dynamically build a flat map of Code -> DevClass from the official schema,
        # ignoring the verb constraint to capture RQ, W, and RP messages.
        hvac_code_map: dict[Code, str] = {}
        for (_, schema_code), dev_class_name in HVAC_KLASS_BY_VC_PAIR.items():
            hvac_code_map[schema_code] = dev_class_name

        dev_class = hvac_code_map.get(code_enum)

        if dev_class:
            # Promote Source (if the device is transmitting, and NOT a controller)
            if msg.src.id != "--:------" and msg.src.type != "01":
                self._emit(
                    TopologyChangedEvent(
                        action=TopologyAction.PROMOTE_CLASS,
                        device_id=msg.src.id,
                        metadata={"device_class": dev_class},
                        causation="Rule_HVAC_Signature_Source",
                    )
                )

            # Promote Target (if the controller is querying/commanding the device)
            if (
                msg.dst.id != "--:------"
                and msg.dst.id != msg.src.id
                and msg.dst.type != "01"
            ):
                self._emit(
                    TopologyChangedEvent(
                        action=TopologyAction.PROMOTE_CLASS,
                        device_id=msg.dst.id,
                        metadata={"device_class": dev_class},
                        causation="Rule_HVAC_Signature_Target",
                    )
                )

    def _evaluate_dhw_opentherm_rules(self, msg: Message) -> None:
        """Evaluate rules specific to DHW and OpenTherm Bridges.

        OpenTherm Bridges exclusively use 3220. DHW sensors are deduced
        via 1260 and 10A0 packets.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        # Prefix Guard: Prevent cross-promotion (e.g., OTB sending 1260)
        if msg.header.code == Code._3220 and msg.src.type == "10":
            event = TopologyChangedEvent(
                action=TopologyAction.PROMOTE_CLASS,
                device_id=msg.src.id,
                metadata={"device_class": DevType.OTB},
                causation="Rule_OTB_3220_Signature",
            )
            self._emit(event)

        elif msg.header.code in (Code._1260, Code._10A0) and msg.src.type == "07":
            event = TopologyChangedEvent(
                action=TopologyAction.PROMOTE_CLASS,
                device_id=msg.src.id,
                metadata={"device_class": DevType.DHW},
                causation="Rule_DHW_Signature",
            )
            self._emit(event)

    def _evaluate_heating_prefix_rules(self, msg: Message) -> None:
        """Evaluate passive heuristics based purely on hardware prefixes.

        Legacy architecture automatically promoted generic devices into specific
        heating domain subtypes (e.g., TRV, UFC, BDR) the moment their address
        was observed anywhere in the packet (src, dst, or embedded in payload).

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        prefix_map = {
            "02": DevType.UFC,
            "03": DevType.HCW,
            "04": DevType.TRV,
            "12": DevType.THM,
            "13": DevType.BDR,
            "22": DevType.THM,
            "34": DevType.THM,
        }

        # Safe L7 extraction (dropping the legacy _pkt._addrs shim)
        addrs = [msg.src]
        if msg.dst and msg.dst.id != "--:------" and msg.dst.id != msg.src.id:
            addrs.append(msg.dst)

        for addr in addrs:
            if getattr(addr, "type", None) in prefix_map:
                event = TopologyChangedEvent(
                    action=TopologyAction.PROMOTE_CLASS,
                    device_id=addr.id,
                    metadata={"device_class": prefix_map[addr.type]},
                    causation="Rule_Heating_Prefix_Heuristic",
                )
                self._emit(event)

    def _evaluate_appliance_control_sync_rules(self, msg: Message) -> None:
        """Evaluate direct configuration syncs to map the System Relay.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        # Guard: Catch direct commands from the Controller (01) to a Relay (13)
        if msg.src.type == "01" and msg.dst and msg.dst.type == "13":
            # 1100 (Boiler Params) or 10E0/1FC9 (Binding) are direct links
            if msg.header.code in (Code._1100, Code._10E0, Code._1FC9):
                event = TopologyChangedEvent(
                    action=TopologyAction.BIND_DEVICE,
                    parent_id=msg.src.id,
                    child_id=msg.dst.id,
                    metadata={
                        "domain_id": "FC",
                        "child_id": "FC",
                        "device_role": "appliance_control",
                    },
                    causation="Rule_Direct_Relay_Sync",
                )
                self._emit(event)

    def _evaluate_eavesdrop_rules(self, msg: Message) -> None:
        """Evaluate broadcast telemetry for heuristic sensor correlation.

        :param msg: The immutable Message L7 envelope to evaluate.
        """
        if not self._enable_eavesdrop:
            return

        # Catch Controller Sync Array (30C9 from 01 to --:------ or specific)
        if (
            msg.header.verb == I_
            and msg.header.code == Code._30C9
            and msg.src.type == "01"
        ):
            event = TopologyChangedEvent(
                action=TopologyAction.UPDATE_TRAITS,
                device_id=msg.src.id,
                metadata={"eavesdrop": "controller_sync", "payload": str(msg.data)},
                causation="Rule_30C9_Controller_Sync",
            )
            self._emit(event)

        # Catch Orphan Sensor Broadcast (30C9 from sensors to themselves)
        elif (
            msg.header.verb == I_
            and msg.header.code == Code._30C9
            and msg.dst.id == msg.src.id
        ):
            event = TopologyChangedEvent(
                action=TopologyAction.UPDATE_TRAITS,
                device_id=msg.src.id,
                metadata={"eavesdrop": "orphan_broadcast", "payload": str(msg.data)},
                causation="Rule_30C9_Orphan_Broadcast",
            )
            self._emit(event)

    def _evaluate_implicit_binding_rules(self, msg: Message) -> None:
        """Evaluate implicit bindings from directed controller polls.

        If a Controller (01:) explicitly sends a direct command (RQ, W) to a
        heating device (e.g., 04: TRV, 00: Zone Sensor, 08: Relay), it implies
        the controller believes that device belongs to its network.

        :param msg: The immutable Message L7 envelope to evaluate.
        :type msg: Message
        :return: None
        :rtype: None
        """
        if not self._enable_eavesdrop:
            return

        # 1. We only care about explicit, directed requests/writes
        if msg.header.verb not in ("RQ", " W"):
            return

        # 2. The source MUST be a Controller
        if getattr(msg.src, "type", None) != "01":
            return

        # 3. The target MUST be a valid Heating Domain device
        # (00 = Zone Sensor, 04 = TRV, 08 = Relay/BDR91)
        if getattr(msg.dst, "type", None) not in ("00", "04", "08"):
            return

        # Emit the topology mutation event. The downstream Registry
        # will safely process this and ignore it if already bound.
        event = TopologyChangedEvent(
            action=TopologyAction.BIND_DEVICE,
            parent_id=msg.src.id,
            child_id=msg.dst.id,
            metadata={
                "device_role": "actuator" if msg.dst.type in ("04", "08") else "sensor"
            },
            causation="Rule_Implicit_Poll_Binding",
        )
        self._emit(event)

    def _evaluate_third_address_broadcast_rules(self, msg: Message) -> None:
        """Evaluate bindings from the third address field of broadcasts.

        Many heating devices broadcast telemetry (I ---) to no one in
        particular (--:------), but explicitly declare their parent
        Controller in the third address slot of the RF frame.

        :param msg: The immutable Message L7 envelope to evaluate.
        :type msg: Message
        :return: None
        :rtype: None
        """
        if not self._enable_eavesdrop:
            return

        if msg.header.verb != I_:
            return

        # Pure L7 architectural access using the new Domain property
        if getattr(msg.addr3, "type", None) == "01" and getattr(
            msg.src, "type", None
        ) in ("00", "04", "08"):
            event = TopologyChangedEvent(
                action=TopologyAction.BIND_DEVICE,
                parent_id=msg.addr3.id,
                child_id=msg.src.id,
                metadata={
                    "device_role": "actuator"
                    if msg.src.type in ("04", "08")
                    else "sensor"
                },
                causation="Rule_3rd_Address_Declaration",
            )
            self._emit(event)
