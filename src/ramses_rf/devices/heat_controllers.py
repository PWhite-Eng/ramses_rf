"""RAMSES RF - Heating Controller Devices."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Final, cast

from ramses_rf.const import (
    DEV_ROLE_MAP,
    FA,
    RQ,
    SZ_HEAT_DEMAND,
    SZ_RELAY_DEMAND,
    Code,
    DevType,
)
from ramses_rf.entity import Entity
from ramses_rf.models import DeviceTraits
from ramses_rf.schemas import SZ_CIRCUITS
from ramses_rf.topology import Child, Parent
from ramses_tx import Command
from ramses_tx.typing import DeviceIdT, DevIndexT, PayloadT

from .dev_base import DeviceHeat

if TYPE_CHECKING:
    from ramses_rf.systems import Evohome, Zone

    from ..messages import Message


_LOGGER = logging.getLogger(__name__)


class Controller(DeviceHeat):  # CTL (01):
    """The Controller base class."""

    HEAT_DEMAND: Final = SZ_HEAT_DEMAND

    _SLUG = DevType.CTL
    _STATE_ATTR = HEAT_DEMAND

    def __init__(
        self, *args: Any, traits: DeviceTraits | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, traits=traits, **kwargs)

        self.tcs: Evohome | None = None  # TODO: = self?

    def _post_class_promote(self) -> None:
        """Initialize CTL state when promoted in-place from a generic device."""
        self.__dict__.setdefault("tcs", None)

    def _setup_discovery_cmds(self) -> None:
        super()._setup_discovery_cmds()

        if not self.is_faked:
            self.discovery.add_cmd(
                Command.from_attrs(RQ, self.id, Code._2E04, PayloadT("00")),
                60 * 60,  # Poll every 60 minutes after initial startup query
            )


class Programmer(Controller):  # PRG (23):
    """The Controller base class."""

    _SLUG = DevType.PRG

    def _setup_discovery_cmds(self) -> None:
        super()._setup_discovery_cmds()

        if not self.is_faked:
            # PRGs respond to RP for 1090, 10A0, 3EF1
            self.discovery.add_cmd(
                Command.from_attrs(RQ, self.id, Code._1090, PayloadT("00")),
                60 * 60 * 6,  # every 6 hours
            )
            self.discovery.add_cmd(
                Command.from_attrs(RQ, self.id, Code._10A0, PayloadT("00")),
                60 * 60 * 6,
            )


class RfgGateway(DeviceHeat):  # RFG (30:)
    """The RFG100 base class."""

    _SLUG = DevType.RFG

    _STATE_ATTR = None


class UfhController(Parent, DeviceHeat):  # UFC (02):
    """The UFC class, the HCE80 that controls the UFH zones."""

    HEAT_DEMAND: Final = SZ_HEAT_DEMAND

    _SLUG = DevType.UFC
    _STATE_ATTR = HEAT_DEMAND

    _child_id = FA
    _iz_controller = True

    childs: list[Child]  # TODO: check (code so complex, not sure if this is true)

    # 12:27:24.398 067  I --- 02:000921 --:------ 01:191718 3150 002 0360
    # 12:27:24.546 068  I --- 02:000921 --:------ 01:191718 3150 002 065A
    # 12:27:24.693 067  I --- 02:000921 --:------ 01:191718 3150 002 045C
    # 12:27:24.824 059  I --- 01:191718 --:------ 01:191718 3150 002 FC5C
    # 12:27:24.857 067  I --- 02:000921 --:------ 02:000921 3150 006 0060-015A-025C

    def __init__(
        self, *args: Any, traits: DeviceTraits | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, traits=traits, **kwargs)
        self._init_ufh_state()

    def _init_ufh_state(self) -> None:
        """Initialize UFH-specific instance attributes (idempotent)."""
        self.__dict__.setdefault("circuit_by_id", {f"{i:02X}": {} for i in range(8)})

    def _post_class_promote(self) -> None:
        """Initialize UFH state when promoted in-place from a generic device."""
        self._init_ufh_state()

    def _setup_discovery_cmds(self) -> None:
        super()._setup_discovery_cmds()

        # Only RPs are: 0001, 0005/000C, 10E0, 000A/2309 & 22D0

        cmd = Command.from_attrs(
            RQ, self.id, Code._0005, PayloadT(f"00{DEV_ROLE_MAP.UFH}")
        )
        self.discovery.add_cmd(cmd, 60 * 60 * 24)

        # TODO: this needs work
        # if discover_flag & Discover.PARAMS:  # only 2309 has any potential?
        for ufc_idx in getattr(self, "circuit_by_id", {}):
            cmd = Command.get_zone_config(self.id, ufc_idx)
            self.discovery.add_cmd(cmd, 60 * 60 * 6)

            cmd = Command.get_zone_setpoint(self.id, ufc_idx)
            self.discovery.add_cmd(cmd, 60 * 60 * 6)

        for ufc_idx in range(8):
            payload = PayloadT(f"{ufc_idx:02X}{DEV_ROLE_MAP.UFH}")
            cmd = Command.from_attrs(RQ, self.id, Code._000C, payload)
            self.discovery.add_cmd(cmd, 60 * 60 * 24)

    # TODO: should be a private method
    def get_circuit(
        self, cct_idx: str, *, msg: Message | None = None, **schema: Any
    ) -> Any:
        """Return a UFH circuit, create it if required.

        First, use the schema to create/update it, then pass it any msg to handle.

        Circuits are uniquely identified by a UFH controller ID|cct_idx pair.
        If a circuit is created, attach it to this UFC.
        """

        schema = {}  # shrink(SCH_CCT(schema))

        cct = cast("UfhCircuit | None", self.child_by_id.get(cct_idx))
        if not cct:
            cct = UfhCircuit(self, cct_idx)
            self.child_by_id[cct_idx] = cct
            self.childs.append(cct)

        elif schema:
            cct._update_schema(**schema)

        if msg:
            cct._handle_msg(msg)
        return cct

    # @property
    # def circuits(self) -> dict:  # 000C
    #     return self.circuit_by_id

    async def heat_demand(self) -> float | None:  # 3150|FC (there is also 3150|FA)
        state = getattr(self, "demand_state", None)
        return state.heat_demand if state else None

    async def heat_demands(self) -> list[dict[str, Any]] | None:  # 3150|ufh_idx array
        """Return the UFH heat demands.

        # TODO: Refactor for #714 (CQRS API Boundaries).
        # This is a legacy shim to maintain backward compatibility with ramses_cc.
        """
        state = getattr(self, "ufh_state", None)
        if state and state.heat_demands:
            return [
                {"ufx_idx": str(k), "heat_demand": v}
                for k, v in state.heat_demands.items()
            ]
        return None

    async def relay_demand(self) -> float | None:  # 0008|FC
        state = getattr(self, "demand_state", None)
        return state.relay_demand if state else None

    async def relay_demand_fa(self) -> float | None:  # 0008|FA
        state = getattr(self, "ufh_state", None)
        return state.relay_demand_fa if state else None

    async def setpoints(self) -> dict[str, Any] | None:  # 22C9|ufh_idx array
        """Return the UFH setpoints.

        # TODO: Refactor for #714 (CQRS API Boundaries).
        # This is a legacy shim to maintain backward compatibility with ramses_cc.
        """
        state = getattr(self, "ufh_state", None)
        if state is None:
            return None

        # Return the dictionary exactly as is (even if empty `{}`, to match legacy)
        return cast(dict[str, Any], state.setpoints)

    async def schema(self) -> dict[str, Any]:
        base_schema = await super().schema()
        return {
            **base_schema,
            SZ_CIRCUITS: getattr(self, "circuit_by_id", {}),
        }

    async def params(self) -> dict[str, Any]:
        base_params = await super().params()
        return {
            **base_params,
            SZ_CIRCUITS: await self.setpoints(),
        }

    async def status(self) -> dict[str, Any]:
        base_status = await super().status()
        return {
            **base_status,
            SZ_HEAT_DEMAND: await self.heat_demand(),
            SZ_RELAY_DEMAND: await self.relay_demand(),
            f"{SZ_RELAY_DEMAND}_fa": await self.relay_demand_fa(),
        }


class UfhCircuit(Child, Entity):  # FIXME
    """The UFH circuit class (UFC:circuit is much like CTL/TCS:zone).

    NOTE: for circuits, there's a difference between :
     - `self.ctl`: the UFH controller, and
     - `self.tcs.ctl`: the Evohome controller
    """

    _SLUG: str = "CCT"  # previously None, strict Mypy fix
    _STATE_ATTR: str | None = None

    def __init__(self, ufc: UfhController, ufh_idx: str) -> None:
        super().__init__(ufc._gwy)

        # FIXME: gwy.message_store entities must know their parent device ID
        # and their own idx
        self._z_id = ufc.id
        self._z_idx = cast("DevIndexT", ufh_idx)

        self.id: DeviceIdT = cast("DeviceIdT", f"{ufc.id}_{ufh_idx}")

        self.ufc: UfhController = ufc
        self._child_id = ufh_idx

        # TODO: _ctl should be: .ufc? .ctl?
        self._ctl: Controller | None = None
        self._zone: Zone | None = None

    def _update_schema(self, **kwargs: Any) -> None:
        raise NotImplementedError

    @property
    def ufx_idx(self) -> str:
        return str(self._child_id)

    @property
    def zone_idx(self) -> str | None:
        if self._zone:
            return str(self._zone._child_id)
        return None
