"""RAMSES RF - RAMSES-II compatible packet protocol."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime as dt
from typing import TYPE_CHECKING, Any, cast

from ramses_tx.address import HGI_DEVICE_ID

from .. import exceptions as exc
from ..address import ALL_DEV_ADDR, HGI_DEV_ADDR, NON_DEV_ADDR
from ..command import Command
from ..const import (
    DEFAULT_DISABLE_QOS,
    DEFAULT_GAP_DURATION,
    DEFAULT_NUM_REPEATS,
    DEV_TYPE_MAP,
    SZ_ACTIVE_HGI,
    SZ_BLOCK_LIST,
    SZ_CLASS,
    SZ_IS_EVOFW3,
    SZ_KNOWN_LIST,
    DevType,
    Priority,
)
from ..message import Message
from ..models import QosParams
from ..packet import Packet
from .const import _DBG_DISABLE_IMPERSONATION_ALERTS, _DBG_FORCE_LOG_PACKETS
from .fsm import ProtocolContext
from .interfaces import IPacketSender

if TYPE_CHECKING:
    from ..transports import RamsesTransport
    from ..typing import DeviceIdT, DeviceListT, MsgFilterT, MsgHandlerT

_LOGGER = logging.getLogger(__name__)

TIP = f", configure the {SZ_KNOWN_LIST}/{SZ_BLOCK_LIST} as required"


class RamsesProtocol(asyncio.Protocol, IPacketSender):
    """Network Protocol for RAMSES RF.

    This protocol handles the translation between raw transport data and
    domain-specific RAMSES RF objects (Packets/Messages).
    """

    def __init__(
        self,
        msg_handler: MsgHandlerT,
        *,
        disable_qos: bool | None = DEFAULT_DISABLE_QOS,
        disable_sending: bool = False,
        enforce_include_list: bool = False,
        exclude_list: DeviceListT | None = None,
        include_list: DeviceListT | None = None,
    ) -> None:
        """Initialize the RAMSES Protocol.

        :param msg_handler: The primary callback for incoming messages.
        :param disable_qos: If True, disables Quality of Service retries.
        :param disable_sending: If True, blocks all outbound transmissions.
        :param enforce_include_list: If True, drops packets from unknown devices.
        :param exclude_list: Dictionary of explicitly blocked devices.
        :param include_list: Dictionary of explicitly allowed devices.
        """
        self._msg_handler = msg_handler
        self._msg_handlers: list[MsgHandlerT] = []

        self._disable_qos = disable_qos
        self._disable_sending = disable_sending

        # Device ID Filtering
        self._enforce_include_list = enforce_include_list
        self._exclude_list = list((exclude_list or {}).keys())
        self._include_list = list((include_list or {}).keys())
        self._include_list += [ALL_DEV_ADDR.id, NON_DEV_ADDR.id]

        self._transport: RamsesTransport | None = None
        self._loop = asyncio.get_running_loop()

        # Keep strong references to background tasks to prevent GC deletion
        self._background_tasks: set[asyncio.Task[Any]] = set()

        self._context = ProtocolContext(self)

        self._active_hgi: DeviceIdT | None = None
        self._is_evofw3: bool | None = None

        # Determine known HGI from include_list (if present)
        self._known_hgi = self._extract_known_hgi_id(
            include_list or {}, disable_warnings=disable_sending
        )

        # State
        self._pause_writing = False
        self._wait_connection_lost: asyncio.Future[None] | None = None
        self._wait_connection_made: asyncio.Future[RamsesTransport] = (
            self._loop.create_future()
        )

        # Foreign Gateway tracking
        self._foreign_gwys_lst: list[DeviceIdT] = []
        self._foreign_last_run = dt.now().date()

    @property
    def hgi_id(self) -> DeviceIdT:
        """Return the Device ID of the active HGI."""
        if self._transport:
            # extra_info might return None, ensure fallback
            hgi = self._transport.get_extra_info(SZ_ACTIVE_HGI)
            if hgi:
                return cast("DeviceIdT", hgi)
        return self._known_hgi or HGI_DEV_ADDR.id

    def add_handler(
        self,
        msg_handler: MsgHandlerT,
        /,
        *,
        msg_filter: MsgFilterT | None = None,
    ) -> Callable[[], None]:
        """Add a Message handler to the list of such callbacks.

        :param msg_handler: The callback function to execute.
        :param msg_filter: Optional filter to restrict which messages are handled.
        :return: A callback function that unregisters this handler when executed.
        """

        def del_handler() -> None:
            if msg_handler in self._msg_handlers:
                self._msg_handlers.remove(msg_handler)

        if msg_handler not in self._msg_handlers:
            self._msg_handlers.append(msg_handler)

        return del_handler

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Called when the connection to the Transport is established.

        :param transport: The base transport establishing the connection.
        """
        if self._wait_connection_made.done():
            return

        self._transport = cast("RamsesTransport", transport)
        self._wait_connection_made.set_result(self._transport)
        self._wait_connection_lost = self._loop.create_future()

        # Gather HGI info
        self._set_active_hgi(self._transport.get_extra_info(SZ_ACTIVE_HGI))
        self._is_evofw3 = self._transport.get_extra_info(SZ_IS_EVOFW3)

        # Notify FSM
        self._context.connection_made()

        if self._pause_writing:
            self._context.pause_writing()
        else:
            self._context.resume_writing()

        _LOGGER.info(f"Protocol: Connection made: {transport}")

    async def wait_for_connection_made(self, timeout: float = 1.0) -> RamsesTransport:
        """Wait until connection_made() has been invoked.

        :param timeout: Time in seconds to wait before raising TimeoutError.
        :return: The connected RamsesTransport instance.
        :raises exc.TransportError: If the connection is not established in time.
        """
        try:
            return await asyncio.wait_for(self._wait_connection_made, timeout)
        except TimeoutError as err:
            raise exc.TransportError(
                f"Transport did not bind to Protocol within {timeout} secs"
            ) from err

    def connection_lost(self, exc: Exception | None) -> None:
        """Called when the connection to the Transport is lost.

        :param exc: The exception causing the disconnect, or None if intended.
        """
        if not self._wait_connection_lost:
            # Handle cases where connection was never fully established
            if self._wait_connection_made.done():
                self._wait_connection_made = self._loop.create_future()
            return

        if self._wait_connection_lost.done():
            return

        if exc:
            self._wait_connection_lost.set_exception(exc)
        else:
            self._wait_connection_lost.set_result(None)

        self._context.connection_lost(exc)
        self._transport = None

        if exc:
            _LOGGER.warning(f"Protocol: Connection lost: {exc}")
        else:
            _LOGGER.info("Protocol: Connection lost")

    async def wait_for_connection_lost(self, timeout: float = 1.0) -> None:
        """Wait until connection_lost() has been invoked.

        :param timeout: Time in seconds to wait before raising TimeoutError.
        :raises exc.TransportError: If the disconnect does not complete in time.
        """
        if not self._wait_connection_lost:
            return

        try:
            await asyncio.wait_for(self._wait_connection_lost, timeout)
        except TimeoutError as err:
            raise exc.TransportError(
                f"Transport did not unbind from Protocol within {timeout} secs"
            ) from err

    def pause_writing(self) -> None:
        """Pause writing to the transport."""
        self._pause_writing = True
        self._context.pause_writing()

    def resume_writing(self) -> None:
        """Resume writing to the transport."""
        self._pause_writing = False
        self._context.resume_writing()

    def pkt_received(self, pkt: Packet) -> None:
        """Called by the Transport when a Packet is received.

        :param pkt: The parsed Packet received from the transport.
        """
        if not self._is_wanted_addrs(pkt.src.id, pkt.dst.id):
            _LOGGER.debug("%s < Packet excluded by device_id filter", pkt)
            return

        if _DBG_FORCE_LOG_PACKETS:
            _LOGGER.warning(f"Recv'd: {pkt._rssi} {pkt}")
        elif _LOGGER.getEffectiveLevel() > logging.DEBUG:
            _LOGGER.info(f"Recv'd: {pkt._rssi} {pkt}")
        else:
            _LOGGER.debug(f"Recv'd: {pkt._rssi} {pkt}")

        # Pass to FSM
        self._context.pkt_received(pkt)

        # Pass to Message Handler
        try:
            msg = Message(pkt)
        except exc.PacketInvalid:
            return

        # Dispatch to main handler and any dynamically added handlers
        self._loop.call_soon_threadsafe(self._msg_handler, msg)
        for callback in self._msg_handlers:
            self._loop.call_soon_threadsafe(callback, msg)

    async def send_cmd(
        self,
        cmd: Command,
        /,
        *,
        gap_duration: float = DEFAULT_GAP_DURATION,
        num_repeats: int = DEFAULT_NUM_REPEATS,
        priority: Priority = Priority.DEFAULT,
        qos: QosParams | dict[str, Any] | None = None,
    ) -> Packet:
        """Send a Command with Qos (retries, etc.).

        :param cmd: The command to transmit.
        :param gap_duration: Time to wait between repeats.
        :param num_repeats: Number of times to repeat the transmission.
        :param priority: Processing priority for the internal queue.
        :param qos: Quality of service parameters for delivery assurance.
        :return: The generated Packet for the transmitted command.
        :raises RuntimeError: If sending is disabled.
        :raises exc.ProtocolError: If the target is blocked by configuration.
        """
        if self._disable_sending:
            raise RuntimeError("Sending is disabled")

        # Filtering
        if not self._is_wanted_addrs(cmd.src.id, cmd.dst.id, sending=True):
            raise exc.ProtocolError(f"Command excluded by device_id filter: {cmd}")

        # HGI Patching
        cmd = self._patch_cmd_if_needed(cmd)

        # Impersonation Alert
        # We only skip the alert for the generic HGI address,
        # but we DO NOT mutate the command itself!
        if cmd.src.id != self.hgi_id and cmd.src.id != HGI_DEVICE_ID:
            await self._send_impersonation_alert(cmd)

        # Ensure qos is a proper QosParams object
        if qos is None:
            qos = QosParams()
        elif isinstance(qos, dict):
            qos = QosParams(**qos)

        if self._disable_qos:
            pass

        return await self._context.send_cmd(cmd, priority=priority, qos=qos)

    def send_pkt(self, pkt: Packet | Command) -> None:
        """Send a packet/command to the transport. Implements IPacketSender.

        :param pkt: The raw Packet or Command to transmit.
        """
        if self._transport:
            task = self._loop.create_task(self._transport.write_frame(str(pkt)))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    def _patch_cmd_if_needed(self, cmd: Command) -> Command:
        """Patch the command with the actual HGI ID if it uses the default.

        :param cmd: The parsed Command potentially requiring modification.
        :return: The patched Command.
        """
        if (
            self.hgi_id
            and self._is_evofw3
            and cmd.src.id == HGI_DEV_ADDR.id
            and self.hgi_id != HGI_DEV_ADDR.id
        ):
            _LOGGER.debug(
                f"Patching command with active HGI ID: swapped {HGI_DEV_ADDR.id} "
                f"-> {self.hgi_id} for {cmd}"
            )
            # Add command patching logic here if required
        return cmd

    async def _send_impersonation_alert(self, cmd: Command) -> None:
        """Send a puzzle packet warning that impersonation is occurring.

        :param cmd: The command indicating impersonation.
        """
        if _DBG_DISABLE_IMPERSONATION_ALERTS:
            return

        msg = f"{self}: Impersonating device: {cmd.src}, for pkt: {cmd.tx_header}"
        if self._is_evofw3 is False:
            _LOGGER.error(f"{msg}, NB: non-evofw3 gateways can't impersonate!")
        else:
            _LOGGER.info(msg)

    def _warn_foreign_hgi(self, dev_id: DeviceIdT) -> None:
        """Warn if an unknown HGI is detected.

        :param dev_id: The identifier of the detected unknown HGI.
        """
        current_date = dt.now().date()
        if self._foreign_last_run != current_date:
            self._foreign_last_run = current_date
            self._foreign_gwys_lst = []

        if dev_id in self._foreign_gwys_lst:
            return

        _LOGGER.warning(
            f"Device {dev_id} is potentially a Foreign gateway, "
            f"the Active gateway is {self._active_hgi}, "
            f"alternatively, is it a HVAC device?{TIP}"
        )
        self._foreign_gwys_lst.append(dev_id)

    def _is_wanted_addrs(
        self, src_id: DeviceIdT, dst_id: DeviceIdT, sending: bool = False
    ) -> bool:
        """Return True if the packet is not to be filtered out.

        :param src_id: Source device ID.
        :param dst_id: Destination device ID.
        :param sending: Boolean indicating if the check is for an outbound TX.
        :return: True if the addresses are permitted.
        """
        for dev_id in (src_id, dst_id):
            if dev_id in self._exclude_list:
                return False

            if dev_id == self._active_hgi:
                continue

            if dev_id in self._include_list:
                continue

            if sending and dev_id == HGI_DEV_ADDR.id:
                continue

            if self._enforce_include_list:
                return False

            if dev_id[:2] == DEV_TYPE_MAP._hex(DevType.HGI):
                if self._active_hgi:
                    self._warn_foreign_hgi(dev_id)

        return True

    @staticmethod
    def _extract_known_hgi_id(
        include_list: DeviceListT,
        /,
        *,
        disable_warnings: bool = False,
        strict_checking: bool = False,
    ) -> DeviceIdT | None:
        """Return the device_id of the gateway specified in the include_list.

        :param include_list: Dictionary defining permitted hardware.
        :param disable_warnings: Suppress output for missing hardware.
        :param strict_checking: Enforce stricter match validation.
        :return: DeviceID representing the primary HGI, or None.
        """
        explicit_hgis = [
            k
            for k, v in include_list.items()
            if v.get(SZ_CLASS) in (DevType.HGI, DEV_TYPE_MAP[DevType.HGI])
        ]

        if explicit_hgis:
            return explicit_hgis[0]

        return None

    def _set_active_hgi(self, dev_id: Any) -> None:
        """Set the Active Gateway (HGI) device_id.

        :param dev_id: Target string Device ID to bind as primary.
        """
        if not dev_id or self._active_hgi:
            return

        if dev_id not in self._exclude_list:
            self._active_hgi = cast("DeviceIdT", dev_id)

        if dev_id not in self._include_list:
            _LOGGER.warning(
                f"The active gateway '{dev_id}' SHOULD be in the (enforced) {SZ_KNOWN_LIST}"
            )
