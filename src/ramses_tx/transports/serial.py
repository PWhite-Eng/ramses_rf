"""RAMSES RF - Serial port transport implementation."""

from __future__ import annotations

import asyncio
import contextlib
import glob
import logging
import os
import sys
from collections.abc import Iterable
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import serial_asyncio_fast as serial_asyncio
from serial import (  # type: ignore[import-untyped]
    Serial as _Serial,
    SerialException,
    serial_for_url as serial_for_url,
)

from .. import exceptions as exc
from ..command import Command
from ..const import (
    MAX_DUTY_CYCLE_RATE,
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
    limit_duty_cycle,
    track_system_syncs,
)

if TYPE_CHECKING:
    from ..protocol import RamsesProtocolT
    from ..schemas import PortConfigT
    from ..typing import SerPortNameT

    # STUB: Define a safe type stub for Mypy to use within this file
    class Serial:
        name: str
        portstr: str

        def read(self, size: int) -> bytes: ...
        def write(self, data: bytes) -> int | None: ...
        def set_low_latency_mode(self, low_latency: bool) -> None: ...

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
elif sys.platform.lower()[:5] != "linux":
    from serial.tools.list_ports_posix import (  # type: ignore[import-untyped]
        comports as comports,
    )
else:
    from serial.tools.list_ports_linux import SysFS  # type: ignore[import-untyped]

    def list_links(devices: set[str]) -> list[str]:
        links: list[str] = []
        for device in glob.glob("/dev/*") + glob.glob("/dev/serial/by-id/*"):
            if os.path.islink(device) and os.path.realpath(device) in devices:
                links.append(device)
        return links

    def comports(  # type: ignore[no-any-unimported]
        include_links: bool = False, _hide_subsystems: list[str] | None = None
    ) -> list[SysFS]:
        if _hide_subsystems is None:
            _hide_subsystems = ["platform"]
        devices = set()
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
    """Return True if the device attached to the port has the attributes of a Honeywell HGI80."""
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


class _PortTransportAbstractor(asyncio.Transport):
    """Do the bare minimum to abstract a transport from its underlying class."""

    # NOTE: Replaced serial_asyncio.SerialTransport with asyncio.Transport to avoid
    # direct dependency on serial_asyncio implementation details in abstractor

    def __init__(
        self,
        serial_instance: Serial,
        protocol: RamsesProtocolT,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__()
        # In a real implementation we would call serial_asyncio.SerialTransport.__init__
        # but here we rely on PortTransport inheritance structure


class _MroInitShim:
    """Helper to block the MRO chain from reaching SerialTransport.__init__ via super().

    This is necessary because PortTransport manually initializes SerialTransport
    to pass strict arguments, but _BaseTransport (higher in MRO) tries to
    initialize it again with generic arguments, causing a TypeError.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # We deliberately Swallow the init call here.
        # Do NOT call super().__init__ because the next class in the chain
        # is SerialTransport, which requires arguments we don't have here.
        pass


class PortTransport(  # type: ignore[misc]
    _RegHackMixin,
    _FullTransport,
    _MroInitShim,
    serial_asyncio.SerialTransport,
):
    """Send/receive packets async to/from evofw3/HGI80 via a serial port."""

    _init_fut: asyncio.Future[Any]
    _init_task: asyncio.Task[None]
    _recv_buffer: bytes = b""

    # These attributes are dynamically provided by serial_asyncio.SerialTransport
    serial: Serial
    _max_read_size: int

    def __init__(
        self,
        serial_instance: Serial,
        protocol: RamsesProtocolT,
        loop: asyncio.AbstractEventLoop | None = None,
        **kwargs: Any,
    ) -> None:
        # Manually mixin serial_asyncio logic here.
        serial_asyncio.SerialTransport.__init__(
            self, loop or asyncio.get_running_loop(), protocol, serial_instance
        )
        _FullTransport.__init__(self, loop=loop, **kwargs)
        _RegHackMixin.__init__(self, **kwargs)

        self._leaker_sem = asyncio.BoundedSemaphore()
        self._leaker_task = self._loop.create_task(
            self._leak_sem(), name="PortTransport._leak_sem()"
        )
        self._loop.create_task(
            self._create_connection(), name="PortTransport._create_connection()"
        )

    async def _create_connection(self) -> None:
        self._is_hgi80 = await is_hgi80(self.serial.name)

        async def connect_sans_signature() -> None:
            self._init_fut.set_result(None)
            self._make_connection(gwy_id=None)

        async def connect_with_signature() -> None:
            sig = Command._puzzle()
            self._extra[SZ_SIGNATURE] = sig.payload
            num_sends = 0
            while num_sends < SIGNATURE_MAX_TRYS:
                num_sends += 1
                await self._write_frame(str(sig))
                await asyncio.sleep(SIGNATURE_GAP_SECS)
                if self._init_fut.done():
                    pkt = self._init_fut.result()
                    self._make_connection(gwy_id=pkt.src.id if pkt else None)
                    return
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
        while True:
            await asyncio.sleep(MIN_INTER_WRITE_GAP)
            with contextlib.suppress(ValueError):
                self._leaker_sem.release()

    def _read_ready(self) -> None:
        def bytes_read(data: bytes) -> Iterable[tuple[Any, bytes]]:
            self._recv_buffer += data
            if b"\r\n" in self._recv_buffer:
                lines = self._recv_buffer.split(b"\r\n")
                self._recv_buffer = lines[-1]
                for line in lines[:-1]:
                    yield self._dt_now(), line + b"\r\n"

        try:
            data: bytes = self.serial.read(self._max_read_size)
        except SerialException as err:
            if not self._closing:
                self._close(error=err)
            return

        if not data:
            return

        for dtm, raw_line in bytes_read(data):
            if DBG_FORCE_FRAME_LOGGING:
                _LOGGER.warning("Rx: %s", raw_line)
            elif _LOGGER.getEffectiveLevel() == logging.INFO:
                _LOGGER.info("Rx: %s", raw_line)

            self._frame_read(
                dtm.isoformat(timespec="milliseconds"), _normalise(_str(raw_line))
            )

    @track_system_syncs
    def _pkt_read(self, pkt: Any) -> None:
        if (
            not self._init_fut.done()
            and pkt.code == Code._PUZZ
            and pkt.payload == self._extra[SZ_SIGNATURE]
        ):
            self._extra[SZ_ACTIVE_HGI] = pkt.src.id
            self._init_fut.set_result(pkt)

        super()._pkt_read(pkt)

    @limit_duty_cycle(MAX_DUTY_CYCLE_RATE)
    @avoid_system_syncs
    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        await self._leaker_sem.acquire()
        await super().write_frame(frame)

    async def _write_frame(self, frame: str) -> None:
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
        self.serial.write(data)

    def _abort(self, exc: BaseException | None) -> None:
        """Abort the transport connection.

        Overridden to match SerialTransport signature (BaseException).
        """
        super()._abort(exc)
        if self._init_task:
            self._init_task.cancel()
        if self._leaker_task:
            self._leaker_task.cancel()

    def _close(self, error: Exception | None = None) -> None:
        """Close the transport connection.

        Overridden to match SerialTransport signature (Exception | None).
        """
        # We must satisfy the base class signature (Exception | None), but
        # _FullTransport._close expects RamsesException | None.
        if error and isinstance(error, exc.RamsesException):
            super()._close(error)
        else:
            super()._close(None)

        if init_task := getattr(self, "_init_task", None):
            init_task.cancel()
        if self._leaker_task:
            self._leaker_task.cancel()
