"""RAMSES RF - Serial port transport implementation."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import Iterable
from functools import partial
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast

import serial_asyncio_fast as serial_asyncio
from serial import (  # type: ignore[import-untyped]
    Serial as _Serial,
    SerialException,
    serial_for_url,
)

from .. import const, exceptions as exc
from ..command import Command
from ..const import (
    MIN_INTER_WRITE_GAP,
    SIGNATURE_GAP_SECS,
    SIGNATURE_MAX_SECS,
    SIGNATURE_MAX_TRYS,
    SZ_ACTIVE_HGI,
    SZ_SIGNATURE,
    Code,
)
from ..schemas import SCH_SERIAL_PORT_CONFIG
from .base import (
    DBG_FORCE_FRAME_LOGGING,
    _FullTransport,
    _normalise,
    _RegHackMixin,
    _str,
    avoid_system_syncs,
    track_system_syncs,
)

if TYPE_CHECKING:
    from ..packet import Packet
    from ..protocol import RamsesProtocol
    from ..schemas import PortConfigT
    from ..typing import SerPortNameT

    # STUB: Define a safe type stub for Mypy to use within this file
    class Serial:
        name: str
        portstr: str
        fd: int | None  # File descriptor

        def read(self, size: int) -> bytes: ...
        def write(self, data: bytes) -> int | None: ...
        def flush(self) -> None: ...
        def set_low_latency_mode(self, low_latency: bool) -> None: ...
        def close(self) -> None: ...

else:
    # RUNTIME: Map Serial back to the real class so code runs
    Serial = _Serial


# Platform-specific imports for comports
if os.name == "nt":
    from serial.tools.list_ports_windows import (  # type: ignore[import-untyped]
        comports as comports,
    )
elif os.name != "posix":
    raise ImportError(
        f"Sorry: no implementation for your platform ('{os.name}') available"
    )
else:
    # Custom comports implementation for Linux/Posix to support PTYs in tests
    import glob

    from serial.tools.list_ports_linux import SysFS  # type: ignore[import-untyped]

    def list_links(devices: set[str]) -> list[str]:
        links: list[str] = []
        for device in glob.glob("/dev/*") + glob.glob("/dev/serial/by-id/*"):
            if os.path.islink(device) and os.path.realpath(device) in devices:
                links.append(device)
        return links

    def _comports(  # type: ignore[no-any-unimported]
        include_links: bool = False, _hide_subsystems: list[str] | None = None
    ) -> list[SysFS]:
        if _hide_subsystems is None:
            _hide_subsystems = ["platform"]
        devices = set()
        # Use /proc/tty/drivers to find all serial devices, including PTYs
        with open("/proc/tty/drivers") as file:
            drivers = file.readlines()
            for driver in drivers:
                items = driver.strip().split()
                if items[4] == "serial":
                    devices.update(glob.glob(items[1] + "*"))
        if include_links:
            devices.update(list_links(devices))
        result: list[SysFS] = [  # type: ignore[no-any-unimported]
            d for d in map(SysFS, devices) if d.subsystem not in _hide_subsystems
        ]
        return result

    # Assign to module-level variable to allow patching by tests
    comports = _comports


_LOGGER = logging.getLogger(__name__)


def create_serial_port(
    port_name: SerPortNameT, port_config: PortConfigT | None
) -> Serial:
    """Return a configured Serial instance for the given port name and config."""
    config: dict[str, Any] = SCH_SERIAL_PORT_CONFIG(port_config or {})

    try:
        # CAST: Explicitly tell Mypy this untyped return is our safe 'Serial' type
        ser_obj = cast(Serial, serial_for_url(port_name, **config))
    except SerialException as err:
        _LOGGER.error("Failed to open %s (config: %s): %s", port_name, config, err)
        raise exc.TransportSourceInvalid(
            f"Unable to open the serial port: {port_name}"
        ) from err

    # FTDI on Posix/Linux would be a common environment for this library...
    with contextlib.suppress(AttributeError, NotImplementedError, ValueError):
        ser_obj.set_low_latency_mode(True)

    if os.name == "nt" or ser_obj.portstr[:7] in ("rfc2217", "socket:"):
        _LOGGER.warning(
            f"{'Windows' if os.name == 'nt' else 'This type of serial interface'} "
            "is not fully supported by this library: "
            "please don't report any Transport/Protocol errors/warnings, "
            "unless they are reproducible with a standard configuration "
            "(e.g. linux with a local serial port)"
        )

    return ser_obj


async def is_hgi80(serial_port: SerPortNameT) -> bool | None:
    """Return True if device is a Honeywell HGI80, False if evofw3, None if unknown."""
    if serial_port[:7] == "mqtt://":
        return False

    if "://" in serial_port:
        try:
            serial_for_url(serial_port, do_not_open=True)
        except (SerialException, ValueError) as err:
            raise exc.TransportSerialError(
                f"Unable to find {serial_port}: {err}"
            ) from err
        return None

    loop = asyncio.get_running_loop()
    if not await loop.run_in_executor(None, os.path.exists, serial_port):
        raise exc.TransportSerialError(f"Unable to find {serial_port}")

    if "by-id" not in serial_port:
        pass
    elif "TUSB3410" in serial_port:
        return True
    elif "evofw3" in serial_port or "FT232R" in serial_port or "NANO" in serial_port:
        return False

    try:
        loop = asyncio.get_running_loop()
        # Use comports; rely on module-level variable to support test patching
        komports = await loop.run_in_executor(
            None, partial(comports, include_links=True)
        )
    except ImportError as err:
        raise exc.TransportSerialError(f"Unable to find {serial_port}: {err}") from err

    vid = {x.device: x.vid for x in komports}.get(serial_port)
    if vid == 0x10AC:
        return True
    elif vid in (0x0403, 0x1B4F):
        return False

    product = {x.device: getattr(x, "product", None) for x in komports}.get(serial_port)
    if not product:
        pass
    elif "TUSB3410" in product:
        return True
    elif "evofw3" in product or "FT232R" in product or "NANO" in product:
        return False

    _LOGGER.warning(
        f"{serial_port}: the gateway type is not determinable, will assume evofw3"
        + (", TIP: specify the serial port by-id" if "by-id" not in serial_port else "")
    )
    return None


class _SerialProtocolAdapter(asyncio.Protocol):
    """Internal Protocol to bridge serial_asyncio to PortTransport.

    This class runs inside the serial loop and forwards data to the PortTransport.
    It acts as the 'Protocol' in the asyncio Transpot/Protocol pair.
    """

    def __init__(self, transport_wrapper: PortTransport) -> None:
        self._ramses_transport = transport_wrapper
        self._transport: asyncio.BaseTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport
        # Note: We do NOT call ramses_transport.connection_made here.
        # PortTransport manages its own high-level connection lifecycle.

    def data_received(self, data: bytes) -> None:
        # Forward raw bytes immediately to the high-level transport
        if _LOGGER.getEffectiveLevel() <= logging.DEBUG:
            _LOGGER.debug(f"ADAPTER: data_received({len(data)} bytes)")
        self._ramses_transport._on_data_received(data)

    def connection_lost(self, error: Exception | None) -> None:
        # Notify the parent transport of the loss
        if not self._ramses_transport.is_closing():
            # Use 'error' here so we don't shadow the 'exc' module import
            self._ramses_transport._close(exc=cast(exc.RamsesException, error))
        self._transport = None


class _PortTransportAbstractor:
    """Consumes arguments at the end of the MRO chain.

    This prevents TypeError: object.__init__() takes exactly one argument,
    because _BaseTransport blindly forwards kwargs to object.__init__.
    """

    def __init__(
        self, loop: asyncio.AbstractEventLoop | None = None, **kwargs: Any
    ) -> None:
        pass


class PortTransport(_RegHackMixin, _FullTransport, _PortTransportAbstractor):
    """Send/receive packets async to/from evofw3/HGI80 via a serial port."""

    _init_fut: asyncio.Future[Any]
    _init_task: asyncio.Task[None]
    _recv_buffer: bytes = b""

    # Underlying asyncio transport
    _serial_transport: asyncio.Transport | None = None

    def __init__(
        self,
        serial_instance: Serial,
        protocol: RamsesProtocol,
        loop: asyncio.AbstractEventLoop | None = None,
        **kwargs: Any,
    ) -> None:
        # 1. Capture port-specific info
        self._serial_instance = serial_instance

        # 2. Pass protocol explicitly as the FIRST argument to the MRO chain
        super().__init__(protocol, loop=loop, **kwargs)

        self._serial_instance = serial_instance
        self._protocol = protocol
        self._loop = loop or asyncio.get_running_loop()

        self._leaker_sem = asyncio.BoundedSemaphore()
        self._leaker_task = self._loop.create_task(
            self._leak_sem(), name="PortTransport._leak_sem()"
        )

        # Duty Cycle tracking
        self._bits_in_bucket = (
            const.MAX_DUTY_CYCLE_RATE * 38400 * const.DUTY_CYCLE_DURATION
        )
        self._last_time_bit_added = perf_counter()

        # Start the connection process
        self._loop.create_task(
            self._create_connection(), name="PortTransport._create_connection()"
        )

    async def _create_connection(self) -> None:
        """Establish the serial connection and perform handshake."""
        # 1. Manually instantiate SerialTransport to wrap the existing instance.
        try:
            adapter = _SerialProtocolAdapter(self)
            self._serial_transport = serial_asyncio.SerialTransport(
                self._loop, adapter, self._serial_instance
            )

            # Manually trigger the connection_made event on our adapter to link them.
            adapter.connection_made(self._serial_transport)

        except Exception as err:
            _LOGGER.error(f"Failed to create serial connection: {err}")
            self._close(exc=cast(exc.RamsesException, err))
            return

        # 2. Perform HGI80 Detection (Hardware Check)
        detected_hgi80 = await is_hgi80(self._serial_instance.name)

        if self._evofw_flag is not None:
            self._is_hgi80 = not self._evofw_flag
        elif detected_hgi80 is not None:
            self._is_hgi80 = detected_hgi80
        else:
            # Fallback for unknown PTYs (logs warning in is_hgi80)
            self._is_hgi80 = False

        # 3. Handle Signature/Handshake (Protocol Check)
        async def connect_sans_signature() -> None:
            self._init_fut.set_result(None)
            self._make_connection(gwy_id=None)

        async def connect_with_signature() -> None:
            sig = Command._puzzle()
            self._extra[SZ_SIGNATURE] = sig.payload
            num_sends = 0

            # Send signature commands until we get a reply or run out of tries
            while num_sends < SIGNATURE_MAX_TRYS:
                num_sends += 1
                await self._write_frame(str(sig))
                await asyncio.sleep(SIGNATURE_GAP_SECS)

                if self._init_fut.done():
                    pkt = self._init_fut.result()
                    self._make_connection(gwy_id=pkt.src.id if pkt else None)
                    return

            # If we timed out on signatures, assume no HGI/Evofw3 logic required
            if not self._init_fut.done():
                self._init_fut.set_result(None)
            self._make_connection(gwy_id=None)

        self._init_fut = asyncio.Future()

        if self._disable_sending:
            self._init_task = self._loop.create_task(
                connect_sans_signature(), name="PortTransport.connect_sans_signature()"
            )
        else:
            self._init_task = self._loop.create_task(
                connect_with_signature(), name="PortTransport.connect_with_signature()"
            )

        try:
            await asyncio.wait_for(self._init_fut, timeout=SIGNATURE_MAX_SECS)
        except TimeoutError as err:
            raise exc.TransportSerialError(
                f"Failed to initialise Transport within {SIGNATURE_MAX_SECS} secs"
            ) from err

    async def _leak_sem(self) -> None:
        """Release the semaphore at fixed intervals to rate-limit writes."""
        while True:
            await asyncio.sleep(MIN_INTER_WRITE_GAP)
            with contextlib.suppress(ValueError):
                self._leaker_sem.release()

    def _on_data_received(self, data: bytes) -> None:
        """Internal callback: Process raw bytes received from the adapter."""

        def bytes_read(recv_data: bytes) -> Iterable[tuple[Any, bytes]]:
            self._recv_buffer += recv_data
            if b"\r\n" in self._recv_buffer:
                lines = self._recv_buffer.split(b"\r\n")
                self._recv_buffer = lines[-1]
                for line in lines[:-1]:
                    yield self._dt_now(), line + b"\r\n"

        for dtm, raw_line in bytes_read(data):
            if DBG_FORCE_FRAME_LOGGING:
                _LOGGER.warning("Rx: %s", raw_line)
            elif _LOGGER.getEffectiveLevel() == logging.INFO:
                _LOGGER.info("Rx: %s", raw_line)

            self._frame_read(
                dtm.isoformat(timespec="milliseconds"), _normalise(_str(raw_line))
            )

    @track_system_syncs
    def _pkt_read(self, pkt: Packet) -> None:
        """Handle incoming packets, checking for handshake signatures."""
        if (
            not self._init_fut.done()
            and pkt.code == Code._PUZZ
            and pkt.payload == self._extra[SZ_SIGNATURE]
        ):
            self._extra[SZ_ACTIVE_HGI] = pkt.src.id
            self._init_fut.set_result(pkt)

        super()._pkt_read(pkt)

    @avoid_system_syncs
    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        """Write a frame to the transport, respecting duty cycles and syncs."""
        # 1. Enforce minimum gap between writes
        # SKIP if disable_tx_limits is True OR if running in Virtual RF mode (tests)
        if not disable_tx_limits and "virtual_rf" not in self._extra:
            await self._leaker_sem.acquire()

        # 2. Enforce Duty Cycle (Token Bucket) logic manually
        # Note: We do this manually here (instead of using the decorator) to:
        # a) Respect 'disable_tx_limits' (which the base decorator ignores)
        # b) Use the dynamic value of const.MAX_DUTY_CYCLE_RATE (allowing test patches)

        if (
            not disable_tx_limits
            and not const.DBG_DISABLE_DUTY_CYCLE_LIMIT
            and "virtual_rf" not in self._extra
        ):
            tx_rate_avail: int = 38400
            fill_rate: float = tx_rate_avail * const.MAX_DUTY_CYCLE_RATE
            bucket_capacity: float = fill_rate * const.DUTY_CYCLE_DURATION

            # Calculate frame size in bits (approx 330 bits header + 10 bits/char payload)
            rf_frame_size = 330 + len(frame[46:]) * 10

            # Top-up the bucket
            elapsed_time = perf_counter() - self._last_time_bit_added
            self._bits_in_bucket = min(
                self._bits_in_bucket + elapsed_time * fill_rate, bucket_capacity
            )
            self._last_time_bit_added = perf_counter()

            # Wait if bucket is empty
            if self._bits_in_bucket < rf_frame_size:
                sleep_time = (rf_frame_size - self._bits_in_bucket) / fill_rate
                if sleep_time > 0.001:  # Avoid tiny sleeps
                    await asyncio.sleep(sleep_time)

            # Consume bits
            self._bits_in_bucket -= rf_frame_size

        await super().write_frame(frame)

    async def _write_frame(self, frame: str) -> None:
        """Write the frame bytes to the underlying transport."""
        data = bytes(frame, "ascii") + b"\r\n"
        log_msg = f"Serial transport transmitting frame: {frame}"

        if DBG_FORCE_FRAME_LOGGING:
            _LOGGER.warning(log_msg)
        elif _LOGGER.getEffectiveLevel() > logging.DEBUG:
            _LOGGER.info(log_msg)
        else:
            _LOGGER.debug(log_msg)

        try:
            self._write(data)
        except SerialException as err:
            self._abort(err)

    def _write(self, data: bytes) -> None:
        """Low-level write to the asyncio transport."""
        if self._serial_transport:
            if _LOGGER.getEffectiveLevel() <= logging.DEBUG:
                _LOGGER.debug(f"WRITING: {len(data)} bytes")
            self._serial_transport.write(data)
        else:
            _LOGGER.warning("Attempted to write to closed transport")

    def _abort(self, exc: BaseException | None) -> None:
        """Abort the transport connection immediately."""
        if self._serial_transport:
            # SerialTransport doesn't always have abort, use close if missing
            if hasattr(self._serial_transport, "abort"):
                self._serial_transport.abort()
            else:
                self._serial_transport.close()

        if self._init_task:
            self._init_task.cancel()
        if self._leaker_task:
            self._leaker_task.cancel()

    def _close(self, exc: exc.RamsesException | None = None) -> None:
        """Close the transport connection gracefully."""
        if self._serial_transport:
            self._serial_transport.close()
            self._serial_transport = None

        super()._close(exc)

        if init_task := getattr(self, "_init_task", None):
            init_task.cancel()
        if self._leaker_task:
            self._leaker_task.cancel()
