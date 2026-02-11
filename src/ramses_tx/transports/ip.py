"""RAMSES RF - IP/Callback transport implementation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from .. import exceptions as exc
from ..helpers import dt_now
from .base import _FullTransport

if TYPE_CHECKING:
    from ..protocol import RamsesProtocol

_LOGGER = logging.getLogger(__name__)


class _CallbackTransportAbstractor:
    """Do the bare minimum to abstract a transport from its underlying class."""

    def __init__(
        self, loop: asyncio.AbstractEventLoop | None = None, **kwargs: Any
    ) -> None:
        """Initialize the callback transport abstractor.

        :param loop: The asyncio event loop, defaults to None.
        :type loop: asyncio.AbstractEventLoop | None, optional
        """
        self._loop = loop or asyncio.get_event_loop()
        # Consume 'kwargs' here. Do NOT pass them to object.__init__().
        super().__init__()


class CallbackTransport(_FullTransport, _CallbackTransportAbstractor):
    """A virtual transport that delegates I/O to external callbacks."""

    def __init__(
        self,
        protocol: RamsesProtocol,
        io_writer: Callable[[str], Awaitable[None]],
        disable_sending: bool = False,
        autostart: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(protocol, disable_sending=disable_sending, **kwargs)

        self._protocol = protocol
        self._io_writer = io_writer
        self._reading = False

        _LOGGER.info(f"CallbackTransport created with io_writer={io_writer}")
        self._protocol.connection_made(self)

        if autostart:
            self.resume_reading()

    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        if self._disable_sending:
            raise exc.TransportError("Sending has been disabled")

        _LOGGER.debug(f"Sending frame via external writer: {frame}")

        try:
            await self._io_writer(frame)
        except Exception as err:
            _LOGGER.error(f"External writer failed to send frame: {err}")
            raise exc.TransportError(f"External writer failed: {err}") from err

    async def _write_frame(self, frame: str) -> None:
        await self.write_frame(frame)

    def receive_frame(self, frame: str, dtm: str | None = None) -> None:
        _LOGGER.debug(
            f"Received frame from external source: frame={repr(frame)}, timestamp={dtm}"
        )

        if not self._reading:
            _LOGGER.debug(f"Dropping received frame (transport paused): {repr(frame)}")
            return

        dtm = dtm or dt_now().isoformat()
        _LOGGER.debug(
            f"Ingesting frame into transport: frame={repr(frame)}, timestamp={dtm}"
        )
        self._frame_read(dtm, frame.rstrip())
