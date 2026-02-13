#!/usr/bin/env python3

# TODO:
# - self._tasks is not ThreadSafe


"""RAMSES RF - The serial to RF gateway (HGI80, not RFG100)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime as dt
from typing import TYPE_CHECKING, Any

from .address import ALL_DEV_ADDR, HGI_DEV_ADDR, NON_DEV_ADDR
from .command import Command
from .const import (
    DEFAULT_DISABLE_QOS,
    DEFAULT_GAP_DURATION,
    DEFAULT_MAX_RETRIES,
    DEFAULT_NUM_REPEATS,
    DEFAULT_SEND_TIMEOUT,
    DEFAULT_WAIT_FOR_REPLY,
    SZ_ACTIVE_HGI,
    SZ_DISABLE_QOS,
    SZ_DISABLE_SENDING,
    SZ_ENFORCE_KNOWN_LIST,
    SZ_LOG_ALL_MQTT,
    SZ_PACKET_LOG,
    SZ_PORT_CONFIG,
    SZ_PORT_NAME,
    SZ_SQLITE_INDEX,
    Priority,
)
from .message import Message
from .models import QosParams
from .packet import Packet
from .protocol import protocol_factory
from .schemas import select_device_filter_mode
from .transports import transport_factory
from .typing import PktLogConfigT, PortConfigT

from .const import (  # noqa: F401, isort: skip, pylint: disable=unused-import
    I_,
    RP,
    RQ,
    W_,
    Code,
)

if TYPE_CHECKING:
    from .const import VerbT
    from .frame import PayloadT
    from .protocol import RamsesProtocol
    from .transports import RamsesTransport
    from .typing import DeviceIdT, DeviceListT

_MsgHandlerT = Callable[[Message], None]


DEV_MODE = False

_LOGGER = logging.getLogger(__name__)


class Engine:
    """The engine class."""

    def __init__(
        self,
        port_name: str | None,
        input_file: str | None = None,
        **kwargs: Any,
    ) -> None:

        # 1. Create Config
        # Allow parameters to be passed as a dict, e.g. config={}
        if "config" in kwargs and isinstance(kwargs["config"], dict):
            kwargs.update(kwargs.pop("config"))

        # 2. Validation
        if not port_name and not input_file:
            raise TypeError("Either a port_name or an input_file must be specified")

        if port_name and input_file:
            _LOGGER.warning(
                "Port (%s) specified, so file (%s) ignored", port_name, input_file
            )
            # We strictly prioritize the port if both are given, but input_file
            # is kept in config for logging purposes if needed.

        self.ser_name = port_name
        self._input_file = input_file

        # 3. Use Config values instead of loose attributes
        self._loop = kwargs.pop("loop", None) or asyncio.get_running_loop()

        self._disable_sending = kwargs.pop(SZ_DISABLE_SENDING, False)

        # Access typed dicts from config
        self._port_config: PortConfigT = kwargs.pop(SZ_PORT_CONFIG, {})
        self._packet_log: PktLogConfigT = kwargs.pop(SZ_PACKET_LOG, {})

        # Ensure lists are iterables (dict) to prevent TypeError in check_filter_lists
        self._exclude: DeviceListT = kwargs.pop("block_list", {}) or {}
        self._include: DeviceListT = kwargs.pop("known_list", {}) or {}

        self._unwanted: list[DeviceIdT] = [
            NON_DEV_ADDR.id,
            ALL_DEV_ADDR.id,
            "01:000001",  # type: ignore[list-item]  # why this one?
        ]

        # Use .get() instead of .pop() to ensure these flags remain in self._kwargs
        # for the transport factory if needed
        self._enforce_known_list = select_device_filter_mode(
            kwargs.get(SZ_ENFORCE_KNOWN_LIST, False),
            self._include,
            self._exclude,
        )
        self._sqlite_index = kwargs.pop(
            SZ_SQLITE_INDEX, False
        )  # TODO Q1 2026: default True
        self._log_all_mqtt = kwargs.pop(SZ_LOG_ALL_MQTT, False)

        # We keep _kwargs only for extra transport params (like auth, etc)
        self._kwargs: dict[str, Any] = kwargs  # HACK

        self._hgi_id = kwargs.get("hgi_id")
        if self._hgi_id:
            self._kwargs[SZ_ACTIVE_HGI] = self._hgi_id

        self._engine_lock = asyncio.Lock()
        self._engine_state: (
            tuple[_MsgHandlerT | None, bool | None, *tuple[Any, ...]] | None
        ) = None

        self._protocol: RamsesProtocol
        self._transport: RamsesTransport | None = None  # None until self.start()

        self._prev_msg: Message | None = None
        self._this_msg: Message | None = None

        self._tasks: list[asyncio.Task] = []  # type: ignore[type-arg]

        self._set_msg_handler(self._msg_handler)  # sets self._protocol

    def __str__(self) -> str:
        if self._hgi_id:
            return f"{self._hgi_id} ({self.ser_name})"

        if not self._transport:
            return f"{HGI_DEV_ADDR.id} ({self.ser_name})"

        device_id = self._transport.get_extra_info(
            SZ_ACTIVE_HGI, default=HGI_DEV_ADDR.id
        )
        return f"{device_id} ({self.ser_name})"

    def _dt_now(self) -> dt:
        return self._transport._dt_now() if self._transport else dt.now()

    def _set_msg_handler(self, msg_handler: _MsgHandlerT) -> None:
        """Create an appropriate protocol for the packet source (transport).

        The corresponding transport will be created later.
        """

        # Use .get() for disable_qos so it remains in _kwargs for Transport
        # Some transports might need to know if QoS is disabled
        self._protocol = protocol_factory(
            msg_handler,
            disable_sending=self._disable_sending,
            disable_qos=self._kwargs.get(SZ_DISABLE_QOS, DEFAULT_DISABLE_QOS),
            enforce_include_list=self._enforce_known_list,
            exclude_list=self._exclude,
            include_list=self._include,
        )

    def add_msg_handler(
        self,
        msg_handler: Callable[[Message], None],
        /,
        msg_filter: Callable[[Message], bool] | None = None,
    ) -> None:
        """Create a client protocol for the RAMSES-II message transport.

        The optional filter will return True if the message is to be handled.
        """

        # if msg_filter is not None and not is_callback(msg_filter):
        #     raise TypeError(f"Msg filter {msg_filter} is not a callback")

        if not msg_filter:
            msg_filter = lambda _: True  # noqa: E731
        else:
            raise NotImplementedError

        self._protocol.add_handler(msg_handler, msg_filter=msg_filter)

    async def start(self) -> None:
        """Create a suitable transport for the specified packet source.

        Initiate receiving (Messages) and sending (Commands).
        """

        pkt_source: dict[str, Any] = {}  # [str, dict | str | TextIO]
        if self.ser_name:
            pkt_source[SZ_PORT_NAME] = self.ser_name
            pkt_source[SZ_PORT_CONFIG] = self._port_config
        else:  # if self._input_file:
            pkt_source[SZ_PACKET_LOG] = self._input_file  # filename as string

        transport_kwargs = self._kwargs.copy()

        # HACK: Re-inject config params that might be needed by the transport
        # If ramses_rf.Gateway is used, self.config is a SimpleNamespace with all settings.
        # Some settings (like reduce_processing) might have been filtered out of kwargs
        # by SCH_ENGINE_CONFIG but are needed by transport/dispatcher implicitly.
        if hasattr(self, "config"):
            for key in (
                "disable_discovery",
                "disable_qos",
                "enforce_known_list",
                "evofw_flag",
                "reduce_processing",
                "use_native_ot",
            ):
                if hasattr(self.config, key) and key not in transport_kwargs:
                    transport_kwargs[key] = getattr(self.config, key)

        # incl. await protocol.wait_for_connection_made(timeout=5)
        self._transport = await transport_factory(
            self._protocol,
            disable_sending=self._disable_sending,
            loop=self._loop,
            log_all=self._log_all_mqtt,
            **pkt_source,
            **transport_kwargs,  # HACK: odd/misc params, e.g. comms_params
        )

        await self._protocol.wait_for_connection_made()

        # TODO: should this be removed (if so, pytest all before committing)
        if self._input_file:
            await self._protocol.wait_for_connection_lost(timeout=10.0)

    async def stop(self) -> None:
        """Close the transport (will stop the protocol)."""

        # Shutdown Safety - wait for tasks to clean up
        tasks = [t for t in self._tasks if not t.done()]
        for t in tasks:
            t.cancel()

        if tasks:
            await asyncio.wait(tasks)

        if self._transport:
            self._transport.close()
            await self._protocol.wait_for_connection_lost()

        return None

    async def _pause(self, *args: Any) -> None:
        """Pause the (active) engine or raise a RuntimeError."""
        # Async lock handling
        if self._engine_lock.locked():
            raise RuntimeError("Unable to pause engine, failed to acquire lock")

        await self._engine_lock.acquire()

        if self._engine_state is not None:
            self._engine_lock.release()
            raise RuntimeError("Unable to pause engine, it is already paused")

        self._engine_state = (None, None, tuple())  # aka not None
        self._engine_lock.release()  # is ok to release now

        self._protocol.pause_writing()  # TODO: call_soon()?
        if self._transport:
            self._transport.pause_reading()  # TODO: call_soon()?

        self._protocol._msg_handler, handler = None, self._protocol._msg_handler  # type: ignore[assignment]
        self._disable_sending, read_only = True, self._disable_sending

        self._engine_state = (handler, read_only, *args)

    async def _resume(self) -> tuple[Any]:  # FIXME: not atomic
        """Resume the (paused) engine or raise a RuntimeError."""

        args: tuple[Any]  # mypy

        # Async lock with timeout
        try:
            await asyncio.wait_for(self._engine_lock.acquire(), timeout=0.1)
        except TimeoutError as err:
            raise RuntimeError(
                "Unable to resume engine, failed to acquire lock"
            ) from err

        if self._engine_state is None:
            self._engine_lock.release()
            raise RuntimeError("Unable to resume engine, it is not paused")

        self._protocol._msg_handler, self._disable_sending, *args = self._engine_state  # type: ignore[assignment]
        self._engine_lock.release()

        if self._transport:
            self._transport.resume_reading()
        if not self._disable_sending:
            self._protocol.resume_writing()

        self._engine_state = None

        return args

    def add_task(self, task: asyncio.Task[Any]) -> None:  # TODO: needs a lock?
        # keep a track of tasks, so we can tidy-up
        self._tasks = [t for t in self._tasks if not t.done()]
        self._tasks.append(task)

    @staticmethod
    def create_cmd(
        verb: VerbT, device_id: DeviceIdT, code: Code, payload: PayloadT, **kwargs: Any
    ) -> Command:
        """Make a command addressed to device_id."""

        if [
            k for k in kwargs if k not in ("from_id", "seqn")
        ]:  # FIXME: deprecate QoS in kwargs
            raise RuntimeError("Deprecated kwargs: %s", kwargs)

        return Command.from_attrs(verb, device_id, code, payload, **kwargs)

    async def async_send_cmd(
        self,
        cmd: Command,
        /,
        *,
        gap_duration: float = DEFAULT_GAP_DURATION,
        num_repeats: int = DEFAULT_NUM_REPEATS,
        priority: Priority = Priority.DEFAULT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: float = DEFAULT_SEND_TIMEOUT,
        wait_for_reply: bool | None = DEFAULT_WAIT_FOR_REPLY,
    ) -> Packet:
        """Send a Command and return the corresponding Packet.

        If wait_for_reply is True (*and* the Command has a rx_header), return the
        reply Packet. Otherwise, simply return the echo Packet.

        If the expected Packet can't be returned, raise:
            ProtocolSendFailed: tried to Tx Command, but didn't get echo/reply
            ProtocolError:      didn't attempt to Tx Command for some reason
        """

        qos = QosParams(
            max_retries=max_retries,
            timeout=timeout,
            wait_for_reply=wait_for_reply,
        )

        # adjust priority, WFR here?
        # if cmd.code in (Code._0005, Code._000C) and qos.wait_for_reply is None:
        #     qos.wait_for_reply = True

        return await self._protocol.send_cmd(
            cmd,
            gap_duration=gap_duration,
            num_repeats=num_repeats,
            priority=priority,
            qos=qos,
        )  # may: raise ProtocolError/ProtocolSendFailed

    def _msg_handler(self, msg: Message) -> None:
        # HACK: This is one consequence of an unpleasant anachronism
        msg.__class__ = Message  # HACK (next line too)
        msg._gwy = self  # type: ignore[assignment]

        self._this_msg, self._prev_msg = msg, self._this_msg
