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
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

from .. import exceptions as exc
from ..const import (
    DBG_DISABLE_DUTY_CYCLE_LIMIT as DBG_DISABLE_DUTY_CYCLE_LIMIT,
    DBG_DISABLE_REGEX_WARNINGS as DBG_DISABLE_REGEX_WARNINGS,
    DBG_FORCE_FRAME_LOGGING as DBG_FORCE_FRAME_LOGGING,
    DEFAULT_TIMEOUT_MQTT as DEFAULT_TIMEOUT_MQTT,
    DEFAULT_TIMEOUT_PORT as DEFAULT_TIMEOUT_PORT,
    DUTY_CYCLE_DURATION,
    I_,
    PKT_LINE_REGEX,
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

if TYPE_CHECKING:
    from ..protocol import RamsesProtocol
    from ..typing import DeviceIdT

_LOGGER = logging.getLogger(__name__)

# Type Definitions
_RegexRuleT: TypeAlias = dict[str, str]


# Protocols for strict Mypy typing of Decorator Factories
class _DutyCycleDecorator(Protocol):
    def __call__[F: Callable[..., Any]](self, fnc: F) -> F: ...


# Constants
_MAX_TRACKED_TRANSMITS = 99
_MAX_TRACKED_DURATION = 300
_MAX_TRACKED_SYNCS = 3


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
) -> _DutyCycleDecorator:
    """Limit the Tx rate to the RF duty cycle regulations (e.g. 1% per hour)."""
    TX_RATE_AVAIL: int = 38400
    FILL_RATE: float = TX_RATE_AVAIL * max_duty_cycle
    BUCKET_CAPACITY: float = FILL_RATE * time_window

    def decorator[F: Callable[..., Any]](fnc: F) -> F:
        bits_in_bucket: float = BUCKET_CAPACITY
        last_time_bit_added = perf_counter()

        @wraps(fnc)
        async def wrapper(self: Any, frame: str, *args: Any, **kwargs: Any) -> None:
            # Honor global test overrides to bypass arbitrary test suite delays
            if kwargs.get("disable_tx_limits") or getattr(
                self, "_disable_tx_limits", False
            ):
                await fnc(self, frame, *args, **kwargs)
                return

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
            return cast(F, wrapper)
        return cast(F, null_wrapper)

    return decorator


def avoid_system_syncs[F: Callable[..., Awaitable[None]]](fnc: F) -> F:
    """Take measures to avoid Tx when any controller is doing a sync cycle."""
    DURATION_PKT_GAP = 0.020
    DURATION_LONG_PKT = 0.022
    DURATION_SYNC_PKT = 0.010

    SYNC_WAIT_LONG = (DURATION_PKT_GAP + DURATION_LONG_PKT) * 2
    SYNC_WAIT_SHORT = DURATION_SYNC_PKT
    SYNC_WINDOW_LOWER = td(seconds=SYNC_WAIT_SHORT * 0.8)
    SYNC_WINDOW_UPPER = SYNC_WINDOW_LOWER + td(seconds=SYNC_WAIT_LONG * 1.2)

    @wraps(fnc)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> None:
        # Honor global test overrides to bypass arbitrary test suite delays
        if kwargs.get("disable_tx_limits") or getattr(
            self, "_disable_tx_limits", False
        ):
            await fnc(self, *args, **kwargs)
            return

        def is_imminent(p: Packet) -> bool:
            return bool(
                SYNC_WINDOW_LOWER
                < (p.dtm + td(seconds=int(p.payload[2:6], 16) / 10) - dt_now())
                < SYNC_WINDOW_UPPER
            )

        start = perf_counter()

        # Safely retrieve local sync cycles, skipping if not yet initialized
        sync_cycles = getattr(self, "_sync_cycles", [])

        while any(is_imminent(p) for p in sync_cycles):
            await asyncio.sleep(SYNC_WAIT_SHORT)

        if perf_counter() - start > SYNC_WAIT_SHORT:
            await asyncio.sleep(SYNC_WAIT_LONG)

        await fnc(self, *args, **kwargs)
        return None

    return cast(F, wrapper)


def track_system_syncs[F: Callable[..., Any]](fnc: F) -> F:
    """Track/remember any new/outstanding TCS sync cycle."""

    @wraps(fnc)
    def wrapper(self: Any, pkt: Packet) -> None:
        def is_pending(p: Packet) -> bool:
            return bool(p.dtm + td(seconds=int(p.payload[2:6], 16) / 10) > dt_now())

        if pkt.code != Code._1F09 or pkt.verb != I_ or pkt._len != 3:
            fnc(self, pkt)
            return None

        # Track locally instead of globally, explicitly typed for Mypy
        current_syncs: deque[Packet] = getattr(
            self, "_sync_cycles", deque(maxlen=_MAX_TRACKED_SYNCS)
        )
        valid_syncs = [p for p in current_syncs if p.src != pkt.src and is_pending(p)]

        new_syncs = deque(valid_syncs, maxlen=_MAX_TRACKED_SYNCS)
        new_syncs.append(pkt)
        self._sync_cycles = new_syncs

        fnc(self, pkt)

    return cast(F, wrapper)


class RamsesTransport(asyncio.Transport):
    """Public Base class for all RAMSES-II transports."""

    def __init__(self, protocol: Any, *args: Any, **kwargs: Any) -> None:
        self._protocol = protocol
        self._sync_cycles: deque[Packet] = deque(maxlen=_MAX_TRACKED_SYNCS)

        extra = kwargs.get("extra")
        kwargs.pop("loop", None)
        super().__init__(extra=extra)

    def _dt_now(self) -> dt:
        """Return the current datetime (overridden by subclasses)."""
        return dt.now()

    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        """Write a frame to the transport."""
        raise NotImplementedError


class _ReadTransport(RamsesTransport):
    """Interface for read-only transports."""

    _protocol: RamsesProtocol = None  # type: ignore[assignment]
    _loop: asyncio.AbstractEventLoop
    _is_hgi80: bool | None = None

    def __init__(
        self,
        protocol: Any,
        *args: Any,
        extra: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._loop = kwargs.pop("loop", None) or asyncio.get_running_loop()
        super().__init__(protocol, *args, **kwargs)

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
        """Return the active event loop."""
        return self._loop

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        if name == SZ_IS_EVOFW3:
            return not self._is_hgi80
        return self._extra.get(name, default)

    def is_closing(self) -> bool:
        """Return True if the transport is closing or closed."""
        return self._closing

    def _close(self, exc: exc.RamsesException | None = None) -> None:
        if self._closing:
            return
        self._closing = True
        self.loop.call_soon_threadsafe(
            functools.partial(self._protocol.connection_lost, exc)
        )

    def close(self) -> None:
        """Close the transport."""
        self._close()

    def is_reading(self) -> bool:
        """Return True if the transport is actively reading data."""
        return self._reading

    def pause_reading(self) -> None:
        """Pause the transport read operations."""
        self._reading = False

    def resume_reading(self) -> None:
        """Resume the transport read operations."""
        self._reading = True

    def _make_connection(self, gwy_id: DeviceIdT | None) -> None:
        self._extra[SZ_ACTIVE_HGI] = gwy_id
        self.loop.call_soon_threadsafe(
            functools.partial(self._protocol.connection_made, self)
        )

    def _frame_read(self, dtm_str: str, frame: str) -> None:
        if not (frame := frame.strip()):
            return

        # Refactor Fix: Use regex to extract RSSI and Packet
        match = PKT_LINE_REGEX.match(frame)
        if match:
            rssi = match.group("rssi") or "---"
            pkt = match.group("pkt")
        else:
            # Fallback (rare) if regex fails but strip passed
            rssi = "---"
            pkt = frame

        # NORMALIZATION: Split to remove extra spaces, fix verb alignment
        pkt_parts = pkt.split()

        if not pkt_parts:
            return

        # Ensure single letter verbs have leading space
        if len(pkt_parts[0]) == 1:
            pkt_parts[0] = f" {pkt_parts[0]}"

        # Reconstruct normalized packet string
        pkt = " ".join(pkt_parts)

        try:
            # Instantiate explicitly passing the separated RSSI
            pkt_obj = Packet.from_file(dtm_str, pkt, rssi=rssi)
        except ValueError as err:
            _LOGGER.debug("%s < PacketInvalid(%s)", frame, err)
            return
        except exc.PacketInvalid as err:
            _LOGGER.warning("%s < PacketInvalid(%s)", frame, err)
            return

        self._pkt_read(pkt_obj)

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
            self._regex_hack(frame, self._outbound_rule),
            disable_tx_limits=disable_tx_limits,
        )
