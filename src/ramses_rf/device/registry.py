"""RAMSES RF - Device Registry."""

from __future__ import annotations

import contextlib
import inspect
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from ramses_rf.address import Address, is_valid_dev_id
from ramses_rf.config import GatewayConfig
from ramses_rf.const import SZ_DEVICES
from ramses_rf.device import DeviceHeat, DeviceHvac, Fakeable
from ramses_rf.enums import TopologyAction
from ramses_rf.exceptions import (
    DeviceNotFaked,
    DeviceNotFoundError,
    SchemaInconsistentError,
)
from ramses_rf.interfaces import DeviceFilterInterface
from ramses_rf.models import DeviceTraits, TopologyChangedEvent
from ramses_rf.schemas import SCH_TRAITS, SZ_ALIAS, SZ_CLASS, SZ_FAKED
from ramses_rf.typing import DeviceIdT, DeviceListT, DeviceTraitsT

if TYPE_CHECKING:
    from ramses_rf.device import Device
    from ramses_rf.messages import Message
    from ramses_rf.systems import Evohome
    from ramses_rf.topology import Parent

_LOGGER = logging.getLogger(__name__)


class DeviceRegistry:
    """Service to manage the registry of known devices."""

    def __init__(
        self,
        device_filter: DeviceFilterInterface,
        config: GatewayConfig,
        device_factory_cb: Callable[[Address, Message | None, DeviceTraits], Device],
    ) -> None:
        """Initialize the DeviceRegistry.

        :param device_filter: The injected filter for validating devices.
        :type device_filter: DeviceFilterInterface
        :param config: The gateway configuration object.
        :type config: GatewayConfig
        :param device_factory_cb: A callback to instantiate domain devices.
        :type device_factory_cb: Callable
        """
        self._device_filter = device_filter
        self._config = config
        self._device_factory_cb = device_factory_cb
        self.devices: list[Device] = []
        self.device_by_id: dict[DeviceIdT, Device] = {}

        # INDUSTRY BEST PRACTICE: The Dispatcher (Command) Pattern
        # --------------------------------------------------------
        # Instead of a long, procedural if/elif chain, we map incoming
        # actions directly to their handler methods. This guarantees O(1)
        # routing speed and makes it incredibly easy for new developers
        # to trace exactly where an event is processed.
        self._event_routers: dict[
            TopologyAction, Callable[[TopologyChangedEvent], None]
        ] = {
            TopologyAction.BIND_DEVICE: self._handle_bind_device,
            TopologyAction.PROMOTE_CLASS: self._handle_promote_class,
            TopologyAction.CREATE_CONTROLLER: self._handle_create_controller,
            TopologyAction.CREATE_CIRCUIT: self._handle_create_circuit,
            TopologyAction.UPDATE_TRAITS: self._handle_update_traits,
        }

    def handle_topology_event(self, event: TopologyChangedEvent) -> None:
        """Process an immutable structural graph mutation event.

        This method acts as the central ingestion point for the Write-Model.
        It looks up the correct handler for the event's action and
        executes it.
        """
        handler = self._event_routers.get(event.action)
        if handler:
            handler(event)
        else:
            _LOGGER.warning(f"No registry handler defined for action: {event.action}")

    def _handle_bind_device(self, event: TopologyChangedEvent) -> None:
        """Bind a child device to a parent domain or zone."""
        if not event.parent_id or not event.child_id:
            return

        parent = self.get_device(event.parent_id)
        if not parent:
            return

        metadata = event.metadata or {}
        is_sensor = bool(metadata.get("is_sensor", False))
        device_role = metadata.get("device_role")

        tcs = getattr(parent, "tcs", parent) if hasattr(parent, "tcs") else parent

        # ROUTING INTERCEPT: Target the correct sub-domain
        if "zone_idx" in metadata:
            zone_idx = str(metadata["zone_idx"])
            if hasattr(tcs, "_get_zone"):
                with contextlib.suppress(Exception):
                    tcs._get_zone(zone_idx)
            elif hasattr(tcs, "_get_htg_zone"):
                with contextlib.suppress(Exception):
                    tcs._get_htg_zone(zone_idx)

            if hasattr(tcs, "get_htg_zone"):
                parent = tcs.get_htg_zone(zone_idx)
            elif hasattr(tcs, "get_zone"):
                parent = tcs.get_zone(zone_idx)
            elif hasattr(tcs, "zone_by_idx") and zone_idx in tcs.zone_by_idx:
                parent = tcs.zone_by_idx[zone_idx]

        elif metadata.get("domain_id") in ("FA", "F9"):
            if hasattr(tcs, "dhw") and tcs.dhw:
                parent = tcs.dhw

        child_domain_id_raw = metadata.get("child_id")
        child_domain_id = (
            str(child_domain_id_raw) if child_domain_id_raw is not None else None
        )

        # 1. FORWARD BINDING (With Legacy Exception Bypass)
        child_dev = None
        try:
            child_dev = self.get_device(
                event.child_id,
                parent=cast("Parent", parent),
                child_id=child_domain_id,
                is_sensor=is_sensor,
            )
        except Exception:
            # THE CULPRIT: The legacy monolith explicitly rejected this pairing!
            # We bypass the strict topology link and fetch the raw device for CQRS tracking.
            child_dev = self.get_device(event.child_id)

        # 2. REVERSE BINDING (Native CQRS Shadow State)
        if child_dev:
            if not getattr(child_dev, "tcs", None) and tcs:
                with contextlib.suppress(AttributeError):
                    child_dev._tcs = tcs  # type: ignore[attr-defined]

            # Dynamically fetch our CQRS shadow maps (bypassing strict Mypy init checks)
            cqrs_acts: dict[str, set[str]] = getattr(self, "_cqrs_actuators", {})
            cqrs_ufcs: set[str] = getattr(self, "_cqrs_ufcs", set())

            dev_type = child_dev.id[:2] if hasattr(child_dev, "id") else None
            is_actuator_hw = dev_type in ("04", "13", "02")

            if tcs and hasattr(tcs, "id"):
                if "zone_idx" in metadata and (
                    device_role == "actuator" or is_actuator_hw
                ):
                    z_key = f"{tcs.id}_{metadata['zone_idx']}"
                    cqrs_acts.setdefault(z_key, set()).add(child_dev.id)

                if device_role == "ufc" or dev_type == "02":
                    cqrs_ufcs.add(child_dev.id)

            # Save the updated shadow state back to the registry
            self._cqrs_actuators = cqrs_acts
            self._cqrs_ufcs = cqrs_ufcs

            # Try to be polite to the legacy object, but don't care if it fails
            if parent:
                if device_role == "actuator" or is_actuator_hw:
                    with contextlib.suppress(Exception):
                        parent._add_actuator(child_dev)  # type: ignore[attr-defined]
                elif is_sensor:
                    with contextlib.suppress(Exception):
                        parent._add_sensor(child_dev)  # type: ignore[attr-defined]
                elif device_role == "ufc" or dev_type == "02":
                    with contextlib.suppress(Exception):
                        parent._add_ufc(child_dev)  # type: ignore[attr-defined]

            _LOGGER.debug(
                f"Bound {event.child_id} to {parent.id} via {event.causation}"
            )

    def _handle_promote_class(self, event: TopologyChangedEvent) -> None:
        """Safely instantiate a promoted class and migrate state."""
        if not event.device_id or not event.metadata:
            return

        old_dev = self.device_by_id.get(event.device_id)
        if not old_dev:
            return

        new_class_slug = str(event.metadata.get("device_class"))
        if not new_class_slug or getattr(old_dev, "_SLUG", None) == new_class_slug:
            return

        # Keep a backup of old traits for rollback
        old_traits_dict = dict(self._config.known_list.get(event.device_id, {}))

        # Update the configuration traits safely
        traits_dict = dict(old_traits_dict)
        traits_dict["class"] = new_class_slug
        self._config.known_list[event.device_id] = traits_dict

        # Pop the old device from the tracking dictionaries to allow the factory
        # to safely call _add_device during __init__ without raising a
        # SchemaInconsistentError
        self.device_by_id.pop(event.device_id, None)
        self.devices = [d for d in self.devices if d.id != event.device_id]

        try:
            # Instantiate the new strict device class via the factory
            traits = DeviceTraits.from_dict(traits_dict)
            new_dev = self._device_factory_cb(old_dev.addr, None, traits)

            # Migrate essential topological state ONLY if a parent existed
            if old_parent := getattr(old_dev, "_parent", None):
                new_dev._apply_topology_link(old_parent)

            # Migrate CQRS Read-Model State (Shadow state preservation)
            if hasattr(old_dev, "temp_state") and hasattr(new_dev, "temp_state"):
                new_dev.temp_state = old_dev.temp_state
            if hasattr(old_dev, "demand_state") and hasattr(new_dev, "demand_state"):
                new_dev.demand_state = old_dev.demand_state

            _LOGGER.info(
                f"Promoted {event.device_id} to {new_class_slug} via {event.causation}"
            )
        except Exception:
            # Rollback on failure: pop the failed new_dev out first
            self.device_by_id.pop(event.device_id, None)
            self.devices = [d for d in self.devices if d.id != event.device_id]
            self._add_device(old_dev)

            # Revert the traits dictionary
            self._config.known_list[event.device_id] = old_traits_dict
            raise

    def _handle_create_controller(self, event: TopologyChangedEvent) -> None:
        """Instruct a device to initialize its Evohome TCS."""
        if not event.device_id:
            return
        dev = self.device_by_id.get(event.device_id)
        if dev and hasattr(dev, "_make_tcs_controller"):
            dev._make_tcs_controller()
            _LOGGER.debug(f"Created Controller on {dev.id} via {event.causation}")

    def _handle_create_circuit(self, event: TopologyChangedEvent) -> None:
        """Instruct a UFH controller to initialize a circuit."""
        if not event.device_id or not event.metadata:
            return
        ufc = self.device_by_id.get(event.device_id)

        if ufc and hasattr(ufc, "get_circuit"):
            ufh_idx = str(event.metadata.get("ufh_idx"))
            circuit = ufc.get_circuit(ufh_idx)

            # REVERSE BINDING: Hydrate the Zone Read-Model with the circuit actuator!
            zone_idx = event.metadata.get("zone_idx")
            if zone_idx and zone_idx != "None" and hasattr(ufc, "tcs"):
                zone = None
                if hasattr(ufc.tcs, "get_htg_zone"):
                    zone = ufc.tcs.get_htg_zone(str(zone_idx))
                elif (
                    hasattr(ufc.tcs, "zone_by_idx")
                    and str(zone_idx) in ufc.tcs.zone_by_idx
                ):
                    zone = ufc.tcs.zone_by_idx[str(zone_idx)]

                if zone and hasattr(zone, "_add_actuator"):
                    zone._add_actuator(circuit)

            _LOGGER.debug(
                f"Created Circuit {ufh_idx} on {ufc.id} via {event.causation}"
            )

    def _handle_update_traits(self, event: TopologyChangedEvent) -> None:
        """Update traits for a specific device (Expansion Hook)."""
        pass

    def _add_device(self, dev: Device) -> None:
        """Add a device to the registry.

        :param dev: The device instance to add.
        :type dev: Device
        :raises SchemaInconsistentError: If the device already exists in
            the registry.
        """
        if dev.id in self.device_by_id:
            raise SchemaInconsistentError(f"Device already exists: {dev.id}")

        self.devices.append(dev)
        self.device_by_id[dev.id] = dev

    def get_device(
        self,
        device_id: DeviceIdT,
        *,
        msg: Message | None = None,
        parent: Parent | None = None,
        child_id: str | None = None,
        is_sensor: bool | None = None,
    ) -> Device:
        """Return a device, creating it if it does not already exist.

        :param device_id: The unique identifier for the device.
        :type device_id: DeviceIdT
        :param msg: An optional initial message for the device to process.
        :type msg: Message | None
        :param parent: The parent entity of this device, if any.
        :type parent: Parent | None
        :param child_id: Specific ID of the child component if applicable.
        :type child_id: str | None
        :param is_sensor: Indicates if this device is treated as a sensor.
        :type is_sensor: bool | None
        :returns: The existing or newly created device instance.
        :rtype: Device
        :raises DeviceNotFoundError: If device ID is blocked or unknown.
        """
        try:
            self._device_filter.check_filter_lists(device_id)
        except DeviceNotFoundError:
            if device_id != self._config.hgi_id:
                raise

        dev = self.device_by_id.get(device_id)

        if not dev:
            # voluptuous bug workaround:
            # https://github.com/alecthomas/voluptuous/pull/524
            _traits_raw: dict[str, Any] = dict(
                self._config.known_list.get(device_id, {})
            )
            _traits_raw.pop("commands", None)

            traits_dict: dict[str, Any] = SCH_TRAITS(
                self._config.known_list.get(device_id, {})
            )
            traits = DeviceTraits.from_dict(traits_dict)

            dev = self._device_factory_cb(Address(device_id), msg, traits)

            if traits.faked:
                if isinstance(dev, Fakeable):
                    dev._make_fake()
                else:
                    _LOGGER.warning(f"The device is not fakeable: {dev}")

        if parent or child_id:
            dev._apply_topology_link(parent, child_id=child_id, is_sensor=is_sensor)

        return dev

    async def fake_device(
        self,
        device_id: DeviceIdT,
        create_device: bool = False,
    ) -> Device | Fakeable:
        """Create a faked device.

        :param device_id: The unique identifier for the device to fake.
        :type device_id: DeviceIdT
        :param create_device: Allow creation if the device does not exist.
        :type create_device: bool
        :returns: The instantiated faked device.
        :rtype: Device | Fakeable
        :raises SchemaInconsistentError: If the provided device ID is invalid.
        :raises DeviceNotFoundError: If the device isn't found or allowed.
        :raises DeviceNotFaked: If the device cannot be faked.
        """
        if not is_valid_dev_id(device_id):
            raise SchemaInconsistentError(f"The device id is not valid: {device_id}")

        known_list = await self.known_list()

        if not create_device and device_id not in self.device_by_id:
            raise DeviceNotFoundError(f"The device id does not exist: {device_id}")
        elif create_device and device_id not in known_list:
            raise DeviceNotFoundError(
                f"The device id is not in the known_list: {device_id}"
            )

        if (dev := self.get_device(device_id)) and isinstance(dev, Fakeable):
            dev._make_fake()
            return cast("Device | Fakeable", dev)

        raise DeviceNotFaked(f"The device is not fakeable: {device_id}")

    async def known_list(self) -> DeviceListT:
        """Return the working known_list (a superset of the provided
        known_list).

        :returns: A dictionary mapping device IDs to their traits.
        :rtype: DeviceListT
        """
        result: dict[str, Any] = {k: v for k, v in self._config.known_list.items()}
        for d in self.devices:
            if (
                not self._config.engine.enforce_known_list
                or d.id in self._config.mac_filter_list
            ):
                traits = await d.traits()
                result[d.id] = cast(
                    DeviceTraitsT,
                    {k: traits.get(k) for k in (SZ_CLASS, SZ_ALIAS, SZ_FAKED)},
                )
        return cast(DeviceListT, result)

    async def params(self) -> dict[str, Any]:
        """Return the parameters for all devices.

        :returns: A dictionary containing parameters for all devices.
        :rtype: dict[str, Any]
        """
        return {SZ_DEVICES: {d.id: await d.params() for d in sorted(self.devices)}}

    async def status(self) -> dict[str, Any]:
        """Return the status for all devices.

        :returns: A dictionary containing device statuses.
        :rtype: dict[str, Any]
        """
        return {SZ_DEVICES: {d.id: await d.status() for d in sorted(self.devices)}}

    @property
    def system_by_id(self) -> dict[DeviceIdT, Evohome]:
        """Return a mapping of device IDs to their associated Evohome systems.

        :returns: Dictionary mapping device ID to Evohome system.
        :rtype: dict[DeviceIdT, Evohome]
        """
        return {
            d.id: d.tcs
            for d in self.devices
            if hasattr(d, "tcs") and getattr(d.tcs, "id", None) == d.id
        }

    @property
    def systems(self) -> list[Evohome]:
        """Return a list of all identified Evohome systems.

        :returns: A list of Evohome instances.
        :rtype: list[Evohome]
        """
        return list(self.system_by_id.values())

    async def get_heat_orphans(self) -> list[DeviceIdT]:
        """Return a list of IDs for orphaned heat devices.

        :returns: A list of device IDs.
        :rtype: list[DeviceIdT]
        """
        orphans = []
        for d in self.devices:
            if (
                not getattr(d, "tcs", None)
                and isinstance(d, DeviceHeat)
                and await d._is_present()
            ):
                orphans.append(d.id)
        return sorted(orphans)

    async def get_hvac_orphans(self) -> list[DeviceIdT]:
        """Return a list of IDs for orphaned HVAC devices.

        :returns: A list of device IDs.
        :rtype: list[DeviceIdT]
        """
        orphans = []
        for d in self.devices:
            if isinstance(d, DeviceHvac) and await d._is_present():
                orphans.append(d.id)
        return sorted(orphans)

    async def generate_schema(self) -> dict[str, Any]:
        """Generate the complete topology schema natively from the CQRS Read-Model.

        This method interrogates the mathematically correct devices and systems
        tracked within the DeviceRegistry to produce a topology dictionary
        matching the legacy Gateway.schema() format. This safely bypasses the
        legacy routing monolith to resolve the split-brain test paradox.

        :returns: A dictionary representing the complete network topology.
        :rtype: dict[str, Any]
        """
        schema: dict[str, Any] = {}
        systems = self.systems
        bound_devices = set()

        if systems:
            schema["main_tcs"] = systems[0].id
            for tcs in systems:
                tcs_schema_func = getattr(tcs, "schema", None)
                if callable(tcs_schema_func):
                    if inspect.iscoroutinefunction(tcs_schema_func):
                        tcs_schema = await tcs_schema_func()
                    else:
                        tcs_schema = tcs_schema_func()
                else:
                    tcs_schema = tcs_schema_func or {}

                # --- APPLY NATIVE CQRS SHADOW STATE ---
                # Inject our perfect mapping to override the legacy object's missing data
                cqrs_acts: dict[str, set[str]] = getattr(self, "_cqrs_actuators", {})
                cqrs_ufcs: set[str] = getattr(self, "_cqrs_ufcs", set())

                for z_idx, z_dict in tcs_schema.get("zones", {}).items():
                    z_key = f"{tcs.id}_{z_idx}"
                    if z_key in cqrs_acts:
                        current = set(z_dict.get("actuators", []))
                        native = cqrs_acts[z_key]
                        if native - current:
                            z_dict["actuators"] = sorted(native.union(current))

                schema[tcs.id] = tcs_schema

                # Extract all successfully bound devices from the generated TCS schema
                for _, zone_data in tcs_schema.get("zones", {}).items():
                    bound_devices.update(zone_data.get("actuators", []))
                    if zone_data.get("sensor"):
                        bound_devices.add(zone_data["sensor"])

                dhw = tcs_schema.get("stored_hotwater", {})
                if dhw:
                    if dhw.get("sensor"):
                        bound_devices.add(dhw["sensor"])
                    if dhw.get("hotwater_valve"):
                        bound_devices.add(dhw["hotwater_valve"])
                    if dhw.get("heating_valve"):
                        bound_devices.add(dhw["heating_valve"])

                ufh = tcs_schema.get("underfloor_heating", {})
                for ufc_id, _ in ufh.items():
                    bound_devices.add(ufc_id)

                app_ctrl = tcs_schema.get("appliance_control")
                if isinstance(app_ctrl, str) and len(app_ctrl) == 9 and ":" in app_ctrl:
                    bound_devices.add(app_ctrl)

                # NATIVE CQRS SCRUBBING: Track devices the legacy system refused to count
                bound_devices.update(cqrs_ufcs)

                # Cleanse TCS-level orphans
                tcs_orphans = tcs_schema.get("orphans", [])
                tcs_schema["orphans"] = [
                    d for d in tcs_orphans if d not in bound_devices
                ]

        else:
            schema["main_tcs"] = None

        # Gather base orphans from the registry
        raw_heat_orphans = await self.get_heat_orphans()
        raw_hvac_orphans = await self.get_hvac_orphans()

        # Filter out ANY device that is definitively bound within the schema
        schema["orphans_heat"] = [d for d in raw_heat_orphans if d not in bound_devices]
        schema["orphans_hvac"] = [d for d in raw_hvac_orphans if d not in bound_devices]

        return schema
