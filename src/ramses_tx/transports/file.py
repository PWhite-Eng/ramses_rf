"""RAMSES RF - File transport implementation."""

from __future__ import annotations

import asyncio
import fileinput
import functools
import logging
from io import TextIOWrapper
from typing import TYPE_CHECKING, Any

from .. import exceptions as exc
from ..const import SZ_READER_TASK
from .base import _ReadTransport

if TYPE_CHECKING:
    from ..protocol import RamsesProtocol

_LOGGER = logging.getLogger(__name__)


class _FileTransportAbstractor:
    """Do the bare minimum to abstract a transport from its underlying class."""

    def __init__(
        self,
        pkt_source: dict[str, str] | str | TextIOWrapper,
        protocol: RamsesProtocol,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._pkt_source = pkt_source
        self._protocol = protocol
        self._loop = loop or asyncio.get_event_loop()


class FileTransport(_ReadTransport, _FileTransportAbstractor):
    """Receive packets from a read-only source such as packet log or a dict."""

    def __init__(
        self,
        pkt_source: Any,
        protocol: RamsesProtocol,
        *args: Any,
        disable_sending: bool = True,
        **kwargs: Any,
    ) -> None:
        # 1. Manually set the source
        self._pkt_source = pkt_source
        self._protocol = protocol
        self._loop = kwargs.get("loop") or asyncio.get_running_loop()

        # 2. Pass protocol as the FIRST argument to _ReadTransport
        super().__init__(protocol, *args, **kwargs)

        if bool(disable_sending) is False:
            raise exc.TransportSourceInvalid("This Transport cannot send packets")

        self._evt_reading = asyncio.Event()

        self._extra[SZ_READER_TASK] = self._reader_task = self._loop.create_task(
            self._start_reader(), name="FileTransport._start_reader()"
        )

        self._make_connection(None)

    async def _start_reader(self) -> None:
        self._reading = True
        self._evt_reading.set()

        try:
            await self._producer_loop()
        except Exception as err:
            self.loop.call_soon_threadsafe(
                functools.partial(self._protocol.connection_lost, err)
            )
        else:
            self.loop.call_soon_threadsafe(
                functools.partial(self._protocol.connection_lost, None)
            )

    def pause_reading(self) -> None:
        self._reading = False
        self._evt_reading.clear()

    def resume_reading(self) -> None:
        self._reading = True
        self._evt_reading.set()

    async def _producer_loop(self) -> None:
        if isinstance(self._pkt_source, dict):
            for dtm_str, pkt_line in self._pkt_source.items():
                await self._process_line(dtm_str, pkt_line)

        elif isinstance(self._pkt_source, str):
            try:
                with fileinput.input(files=self._pkt_source, encoding="utf-8") as file:
                    for dtm_pkt_line in file:
                        await self._process_line_from_raw(dtm_pkt_line)
            except FileNotFoundError as err:
                _LOGGER.warning(f"Correct the packet file name; {err}")

        elif isinstance(self._pkt_source, TextIOWrapper):
            for dtm_pkt_line in self._pkt_source:
                await self._process_line_from_raw(dtm_pkt_line)

        else:
            raise exc.TransportSourceInvalid(
                f"Packet source is not dict, TextIOWrapper or str: {self._pkt_source:!r}"
            )

    async def _process_line_from_raw(self, line: str) -> None:
        if (line := line.strip()) and line[:1] != "#":
            await self._process_line(line[:26], line[27:])

    async def _process_line(self, dtm_str: str, frame: str) -> None:
        await self._evt_reading.wait()
        self._frame_read(dtm_str, frame)
        await asyncio.sleep(0)

    def _close(self, exc: exc.RamsesException | None = None) -> None:
        super()._close(exc)
        if self._reader_task:
            self._reader_task.cancel()
