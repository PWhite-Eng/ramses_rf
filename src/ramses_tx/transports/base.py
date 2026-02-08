"""RAMSES RF - Base transport logic and helpers."""

from __future__ import annotations

import asyncio
import functools
import logging
import re
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime as dt, timedelta as td
from functools import wraps
from string import printable
from time import perf_counter
from typing import TYPE_CHECKING, Any, TypeAlias

from .. import exceptions as exc
from ..const import (
    DUTY_CYCLE_DURATION,
    I_,
    RP,
    RQ,
    SZ_ACTIVE_HGI,
    SZ_EVOFW_FLAG,
    SZ_INBOUND,
    SZ_IS_EVOFW3,
    SZ_OUTBOUND,
    SZ_SIGNATURE,
    W_,
    Code,
)
from ..helpers import dt_now
from ..packet import Packet
from ..schemas import DeviceIdT

if TYPE_CHECKING:
    from ..protocol import RamsesProtocolT


from ..const import (
    DBG_DISABLE_DUTY_CYCLE_LIMIT as DBG_DISABLE_DUTY_CYCLE_LIMIT,
    DBG_DISABLE_REGEX_WARNINGS as DBG_DISABLE_REGEX_WARNINGS,
    DBG_FORCE_FRAME_LOGGING as DBG_FORCE_FRAME_LOGGING,
    DEFAULT_TIMEOUT_MQTT as DEFAULT_TIMEOUT_MQTT,
    DEFAULT_TIMEOUT_PORT as DEFAULT_TIMEOUT_PORT,
)

_LOGGER = logging.getLogger(__name__)


def _normalise(pkt_line: str) -> str:
    """Perform any (transparent) frame-level hacks, as required at (near-)RF layer."""
    pkt_line = re.sub(r"\r\r", "\r", pkt_line)
    if pkt_line[:4] == " 000":
        pkt_line = pkt_line[1:]
    elif pkt_line[:2] in (I_, RQ, RP, W_):
        pkt_line = ""

    if pkt_line[10:14] in (" 08:", " 31:") and pkt_line[-16:] == "* Checksum error":
        pkt_line = pkt_line[:-17] + " # Checksum error (ignored)"

    return pkt_line.strip()


def _str(value: bytes) -> str:
    """Decode bytes to a string, ignoring non-printable characters."""
    try:
        result = "".join(
            c for c in value.decode("ascii", errors="strict") if c in printable
        )
    except UnicodeDecodeError:
        _LOGGER.warning("%s < Can't decode bytestream (ignoring)", value)
        return ""
    return result


def limit_duty_cycle(
    max_duty_cycle: float, time_window: int = DUTY_CYCLE_DURATION
) -> Callable[..., Any]:
    """Limit the Tx rate to the RF duty cycle regulations (e.g. 1% per hour)."""
    TX_RATE_AVAIL: int = 38400
    FILL_RATE: float = TX_RATE_AVAIL * max_duty_cycle
    BUCKET_CAPACITY: float = FILL_RATE * time_window

    def decorator(
        fnc: Callable[..., Awaitable[None]],
    ) -> Callable[..., Awaitable[None]]:
        bits_in_bucket: float = BUCKET_CAPACITY
        last_time_bit_added = perf_counter()

        @wraps(fnc)
        async def wrapper(self: Any, frame: str, *args: Any, **kwargs: Any) -> None:
            nonlocal bits_in_bucket
            nonlocal last_time_bit_added

            rf_frame_size = 330 + len(frame[46:]) * 10
            elapsed_time = perf_counter() - last_time_bit_added
            bits_in_bucket = min(
                bits_in_bucket + elapsed_time * FILL_RATE, BUCKET_CAPACITY
            )
            last_time_bit_added = perf_counter()

            if DBG_DISABLE_DUTY_CYCLE_LIMIT:
                bits_in_bucket = BUCKET_CAPACITY

            if bits_in_bucket < rf_frame_size:
                await asyncio.sleep((rf_frame_size - bits_in_bucket) / FILL_RATE)

            try:
                await fnc(self, frame, *args, **kwargs)
            finally:
                bits_in_bucket -= rf_frame_size

        @wraps(fnc)
        async def null_wrapper(
            self: Any, frame: str, *args: Any, **kwargs: Any
        ) -> None:
            await fnc(self, frame, *args, **kwargs)

        if 0 < max_duty_cycle <= 1:
            return wrapper
        return null_wrapper

    return decorator


_MAX_TRACKED_TRANSMITS = 99
_MAX_TRACKED_DURATION = 300
_MAX_TRACKED_SYNCS = 3
_global_sync_cycles: deque[Packet] = deque(maxlen=_MAX_TRACKED_SYNCS)


def avoid_system_syncs(fnc: Callable[..., Awaitable[None]]) -> Callable[..., Any]:
    """Take measures to avoid Tx when any controller is doing a sync cycle."""
    DURATION_PKT_GAP = 0.020
    DURATION_LONG_PKT = 0.022
    DURATION_SYNC_PKT = 0.010

    SYNC_WAIT_LONG = (DURATION_PKT_GAP + DURATION_LONG_PKT) * 2
    SYNC_WAIT_SHORT = DURATION_SYNC_PKT
    SYNC_WINDOW_LOWER = td(seconds=SYNC_WAIT_SHORT * 0.8)
    SYNC_WINDOW_UPPER = SYNC_WINDOW_LOWER + td(seconds=SYNC_WAIT_LONG * 1.2)

    @wraps(fnc)
    async def wrapper(*args: Any, **kwargs: Any) -> None:
        global _global_sync_cycles

        def is_imminent(p: Packet) -> bool:
            return bool(
                SYNC_WINDOW_LOWER
                < (p.dtm + td(seconds=int(p.payload[2:6], 16) / 10) - dt_now())
                < SYNC_WINDOW_UPPER
            )

        start = perf_counter()

        while any(is_imminent(p) for p in _global_sync_cycles):
            await asyncio.sleep(SYNC_WAIT_SHORT)

        if perf_counter() - start > SYNC_WAIT_SHORT:
            await asyncio.sleep(SYNC_WAIT_LONG)

        await fnc(*args, **kwargs)
        return None

    return wrapper


def track_system_syncs(fnc: Callable[..., None]) -> Callable[..., Any]:
    """Track/remember any new/outstanding TCS sync cycle."""

    @wraps(fnc)
    def wrapper(self: Any, pkt: Packet) -> None:
        global _global_sync_cycles

        def is_pending(p: Packet) -> bool:
            return bool(p.dtm + td(seconds=int(p.payload[2:6], 16) / 10) > dt_now())

        if pkt.code != Code._1F09 or pkt.verb != I_ or pkt._len != 3:
            fnc(self, pkt)
            return None

        _global_sync_cycles = deque(
            p for p in _global_sync_cycles if p.src != pkt.src and is_pending(p)
        )
        _global_sync_cycles.append(pkt)

        if len(_global_sync_cycles) > _MAX_TRACKED_SYNCS:
            _global_sync_cycles.popleft()

        fnc(self, pkt)

    return wrapper


class _BaseTransport:
    """Base class for all transports."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class _ReadTransport(_BaseTransport):
    """Interface for read-only transports."""

    _protocol: RamsesProtocolT = None  # type: ignore[assignment]
    _loop: asyncio.AbstractEventLoop
    _is_hgi80: bool | None = None

    def __init__(
        self, *args: Any, extra: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, loop=kwargs.pop("loop", None))

        self._extra: dict[str, Any] = {} if extra is None else extra
        self._evofw_flag = kwargs.pop(SZ_EVOFW_FLAG, None)
        self._closing: bool = False
        self._reading: bool = False
        self._this_pkt: Packet | None = None
        self._prev_pkt: Packet | None = None

        for key in (SZ_ACTIVE_HGI, SZ_SIGNATURE):
            self._extra.setdefault(key, None)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._protocol})"

    def _dt_now(self) -> dt:
        try:
            return self._this_pkt.dtm  # type: ignore[union-attr]
        except AttributeError:
            return dt(1970, 1, 1, 1, 0)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        if name == SZ_IS_EVOFW3:
            return not self._is_hgi80
        return self._extra.get(name, default)

    def is_closing(self) -> bool:
        return self._closing

    def _close(self, exc: exc.RamsesException | None = None) -> None:
        if self._closing:
            return
        self._closing = True
        self.loop.call_soon_threadsafe(
            functools.partial(self._protocol.connection_lost, exc)  # type: ignore[arg-type]
        )

    def close(self) -> None:
        self._close()

    def is_reading(self) -> bool:
        return self._reading

    def pause_reading(self) -> None:
        self._reading = False

    def resume_reading(self) -> None:
        self._reading = True

    def _make_connection(self, gwy_id: DeviceIdT | None) -> None:
        self._extra[SZ_ACTIVE_HGI] = gwy_id
        self.loop.call_soon_threadsafe(
            functools.partial(self._protocol.connection_made, self, ramses=True)  # type: ignore[arg-type]
        )

    def _frame_read(self, dtm_str: str, frame: str) -> None:
        if not frame.strip():
            return

        try:
            pkt = Packet.from_file(dtm_str, frame)
        except ValueError as err:
            _LOGGER.debug("%s < PacketInvalid(%s)", frame, err)
            return
        except exc.PacketInvalid as err:
            _LOGGER.warning("%s < PacketInvalid(%s)", frame, err)
            return

        self._pkt_read(pkt)

    def _pkt_read(self, pkt: Packet) -> None:
        self._this_pkt, self._prev_pkt = pkt, self._this_pkt
        if self._closing is True:
            raise exc.TransportError("Transport is closing or has closed")
        try:
            self.loop.call_soon_threadsafe(self._protocol.pkt_received, pkt)
        except AssertionError as err:
            _LOGGER.exception("%s < exception from msg layer: %s", pkt, err)
        except exc.ProtocolError as err:
            _LOGGER.error("%s < exception from msg layer: %s", pkt, err)

    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        raise exc.TransportSerialError("This transport is read only")


class _FullTransport(_ReadTransport):
    """Interface representing a bidirectional transport."""

    def __init__(
        self, *args: Any, disable_sending: bool = False, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._disable_sending = disable_sending
        self._transmit_times: deque[dt] = deque(maxlen=_MAX_TRACKED_TRANSMITS)

    def _dt_now(self) -> dt:
        return dt_now()

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        if name == "tx_rate":
            return self._report_transmit_rate()
        return super().get_extra_info(name, default=default)

    def _report_transmit_rate(self) -> float:
        dtm = dt.now() - td(seconds=_MAX_TRACKED_DURATION)
        transmit_times = tuple(t for t in self._transmit_times if t > dtm)
        if len(transmit_times) <= 1:
            return len(transmit_times)
        duration: float = (transmit_times[-1] - transmit_times[0]) / td(seconds=1)
        return int(len(transmit_times) / duration * 6000) / 100

    def _track_transmit_rate(self) -> None:
        self._transmit_times.append(dt.now())
        _LOGGER.debug(f"Current Tx rate: {self._report_transmit_rate():.2f} pkts/min")

    def write(self, data: bytes) -> None:
        raise exc.TransportError("write() not implemented, use write_frame() instead")

    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        if self._disable_sending is True:
            raise exc.TransportError("Sending has been disabled")
        if self._closing is True:
            raise exc.TransportError("Transport is closing or has closed")
        self._track_transmit_rate()
        await self._write_frame(frame)

    async def _write_frame(self, frame: str) -> None:
        raise NotImplementedError("_write_frame() not implemented here")


_RegexRuleT: TypeAlias = dict[str, str]


class _RegHackMixin:
    """Mixin to apply regex rules to inbound and outbound frames."""

    def __init__(
        self, *args: Any, use_regex: dict[str, _RegexRuleT] | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        use_regex = use_regex or {}
        self._inbound_rule: _RegexRuleT = use_regex.get(SZ_INBOUND, {})
        self._outbound_rule: _RegexRuleT = use_regex.get(SZ_OUTBOUND, {})

    @staticmethod
    def _regex_hack(pkt_line: str, regex_rules: _RegexRuleT) -> str:
        if not regex_rules:
            return pkt_line
        result = pkt_line
        for k, v in regex_rules.items():
            try:
                result = re.sub(k, v, result)
            except re.error as err:
                _LOGGER.warning(f"{pkt_line} < issue with regex ({k}, {v}): {err}")
        if result != pkt_line and not DBG_DISABLE_REGEX_WARNINGS:
            _LOGGER.warning(f"{pkt_line} < Changed by use_regex to: {result}")
        return result

    def _frame_read(self, dtm_str: str, frame: str) -> None:
        super()._frame_read(  # type: ignore[misc]
            dtm_str, self._regex_hack(frame, self._inbound_rule)
        )

    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        await super().write_frame(  # type: ignore[misc]
            self._regex_hack(frame, self._outbound_rule)
        )
