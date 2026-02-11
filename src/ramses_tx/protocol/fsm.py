"""RAMSES RF - RAMSES-II compatible packet protocol finite state machine."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime as dt
from queue import Empty, Full, PriorityQueue
from typing import TYPE_CHECKING, Final, TypeAlias

from ..command import Command
from ..const import Priority
from ..models import QosParams
from ..packet import Packet
from .const import (
    _DBG_MAINTAIN_STATE_CHAIN,
    DEFAULT_BUFFER_SIZE,
    DEFAULT_ECHO_TIMEOUT,
    DEFAULT_RPLY_TIMEOUT,
    MAX_RETRY_LIMIT,
    MAX_SEND_TIMEOUT,
)
from .exceptions import (
    ProtocolError,
    ProtocolFsmError,
    ProtocolSendFailed,
    RamsesException,
)

if TYPE_CHECKING:
    from .interfaces import IPacketSender

_LOGGER = logging.getLogger(__name__)

_FutureT: TypeAlias = asyncio.Future[Packet]
# Priority, dt, Command, QosParams, Future
_QueueEntryT: TypeAlias = tuple[Priority, dt, Command, QosParams, _FutureT]


class ProtocolContext:
    """The context for the Protocol FSM.

    Manages the queue of commands waiting to be sent and the current state
    of the protocol (e.g. Idle, WaitEcho).
    """

    SEND_TIMEOUT_LIMIT: Final[float] = MAX_SEND_TIMEOUT

    def __init__(
        self,
        protocol: IPacketSender,
        /,
        *,
        echo_timeout: float = DEFAULT_ECHO_TIMEOUT,
        reply_timeout: float = DEFAULT_RPLY_TIMEOUT,
        max_retry_limit: int = MAX_RETRY_LIMIT,
        max_buffer_size: int = DEFAULT_BUFFER_SIZE,
    ) -> None:
        self._protocol = protocol
        self.echo_timeout = echo_timeout
        self.reply_timeout = reply_timeout
        self.max_retry_limit = min(max_retry_limit, MAX_RETRY_LIMIT)
        self.max_buffer_size = min(max_buffer_size, DEFAULT_BUFFER_SIZE)

        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()
        self._fut: _FutureT | None = None
        self._que: PriorityQueue[_QueueEntryT] = PriorityQueue(
            maxsize=self.max_buffer_size
        )

        self._expiry_timer: asyncio.Task[None] | None = None
        self._multiplier = 0
        self._state: _ProtocolState = Inactive(self)

        self._cmd: Command | None = None
        self._qos: QosParams | None = None
        self._cmd_tx_count: int = 0
        self._cmd_tx_limit: int = 0

    def __repr__(self) -> str:
        msg = f"<ProtocolContext state={self._state}"
        if self._cmd is None:
            return msg + ">"
        if self._cmd_tx_count == 0:
            return msg + ", tx_count=0/0>"
        return msg + f", tx_count={self._cmd_tx_count}/{self._cmd_tx_limit}>"

    @property
    def state(self) -> _ProtocolState:
        return self._state

    def connection_made(self) -> None:
        """Notify the FSM that the connection is made."""
        self._state.connection_made()

    def connection_lost(self, exc: Exception | None) -> None:
        """Notify the FSM that the connection is lost."""
        self._state.connection_lost(exc)

    def pause_writing(self) -> None:
        """Pause writing to the transport."""
        self._state.writing_paused()

    def resume_writing(self) -> None:
        """Resume writing to the transport."""
        self._state.writing_resumed()

    async def send_cmd(
        self,
        cmd: Command,
        priority: Priority,
        qos: QosParams,
    ) -> Packet:
        """Submit a command to the queue to be sent."""
        if isinstance(self._state, Inactive):
            raise ProtocolSendFailed(f"{self}: Send failed (no active transport?)")

        fut: _FutureT = self._loop.create_future()

        try:
            self._que.put_nowait((priority, dt.now(), cmd, qos, fut))
        except Full as err:
            fut.cancel("Send buffer overflow")
            raise ProtocolSendFailed(f"{self}: Send buffer overflow") from err

        if isinstance(self._state, IsInIdle):
            self._loop.call_soon_threadsafe(self._check_buffer_for_cmd)

        # needs to be greater than worse-case via set_state engine
        timeout = min(qos.timeout, self.SEND_TIMEOUT_LIMIT)

        try:
            await asyncio.wait_for(fut, timeout=timeout)
        except TimeoutError as err:  # incl. fut.cancel()
            msg = f"{self}: Expired global timer after {timeout} sec"
            _LOGGER.warning(
                "TOUT.. = %s: send_timeout=%s (%s)", self, timeout, self._cmd is cmd
            )
            if self._cmd is cmd:
                # set_exception() will cause InvalidStateError
                self.set_state(IsInIdle, expired=True)
            raise ProtocolSendFailed(msg) from err

        try:
            return fut.result()
        except ProtocolSendFailed:
            raise
        except (ProtocolError, RamsesException) as err:  # incl. ProtocolFsmError
            raise ProtocolSendFailed(f"{self}: Send failed: {err}") from err

    def pkt_received(self, pkt: Packet) -> None:
        """Process a received packet through the current state."""
        self._state.pkt_rcvd(pkt)

    def set_state(
        self,
        state_class: type[_ProtocolState],
        expired: bool = False,
        timed_out: bool = False,
        exception: Exception | None = None,
        result: Packet | None = None,
    ) -> None:
        """Transition to a new state."""

        async def expire_state_on_timeout() -> None:
            # a separate coro, so can be spawned off with create_task()
            assert self._cmd is not None  # mypy

            if isinstance(self._state, WantEcho):  # otherwise is WantRply
                delay = self.echo_timeout * (2**self._multiplier)
            else:  # isinstance(self._state, WantRply):
                delay = self.reply_timeout * (2**self._multiplier)

            # assuming success, multiplier can be decremented...
            self._multiplier, old_val = max(0, self._multiplier - 1), self._multiplier

            await asyncio.sleep(delay)  # ideally, will be interrupted by wait_for()

            # nope, was not successful, so multiplier should be incremented...
            self._multiplier = min(3, old_val + 1)

            if self._cmd_tx_count < 3:
                level = logging.DEBUG
            elif self._cmd_tx_count == 3:
                level = logging.INFO
            else:
                level = logging.WARNING

            if isinstance(self._state, WantEcho):
                _LOGGER.log(
                    level, f"Timeout expired waiting for echo: {self} (delay={delay})"
                )
            else:
                _LOGGER.log(
                    level, f"Timeout expired waiting for reply: {self} (delay={delay})"
                )

            # Timer has expired, can we retry or are we done?
            if self._cmd_tx_count < self._cmd_tx_limit:
                self.set_state(WantEcho, timed_out=True)
            else:
                self.set_state(IsInIdle, expired=True)

        def effect_state(timed_out_arg: bool) -> None:
            """Take any actions indicated by state, and optionally set expiry timer."""
            if timed_out_arg:
                assert self._cmd is not None, f"{self}: Coding error"
                self._send_cmd(self._cmd, is_retry=True)

            if isinstance(self._state, IsInIdle):
                self._loop.call_soon_threadsafe(self._check_buffer_for_cmd)

            elif isinstance(self._state, WantRply) and not self._qos.wait_for_reply:  # type: ignore[union-attr]
                self.set_state(IsInIdle, result=self._state._echo_pkt)

            elif isinstance(self._state, WantEcho | WantRply):
                self._expiry_timer = self._loop.create_task(expire_state_on_timeout())

        if self._expiry_timer is not None:
            self._expiry_timer.cancel("Changing state")
            self._expiry_timer = None

        # Determine transition details for logging
        current_state_name = self._state.__class__.__name__
        new_state_name = state_class.__name__
        transition = f"{current_state_name}->{new_state_name}"

        # Handle Future (resolve/cancel/except)
        if self._fut is None:
            _LOGGER.debug(
                f"FSM state changed {transition}: no active future (ctx={self})"
            )

        elif self._fut.cancelled() and not isinstance(self._state, IsInIdle):
            _LOGGER.debug(
                f"FSM state changed {transition}: future cancelled "
                f"(expired={expired}, ctx={self})"
            )

        elif exception:
            _LOGGER.debug(
                f"FSM state changed {transition}: exception occurred "
                f"(error={exception}, ctx={self})"
            )
            if not self._fut.done():
                self._fut.set_exception(exception)

        elif result:
            _LOGGER.debug(
                f"FSM state changed {transition}: result received "
                f"(result={result._hdr}, ctx={self})"
            )
            if not self._fut.done():
                self._fut.set_result(result)

        elif expired:
            _LOGGER.debug(f"FSM state changed {transition}: timer expired (ctx={self})")
            if not self._fut.done():
                self._fut.set_exception(
                    ProtocolSendFailed(f"{self}: Exceeded maximum retries")
                )

        else:
            _LOGGER.debug(f"FSM state changed {transition}: successful (ctx={self})")

        prev_state = self._state
        self._state = state_class(self)

        if _DBG_MAINTAIN_STATE_CHAIN:
            setattr(self._state, "_prev_state", prev_state)  # noqa: B010

        # Update counters
        if timed_out:
            self._cmd_tx_count += 1
        elif isinstance(self._state, WantEcho):
            self._cmd_tx_count = 1
        elif not isinstance(self._state, WantRply):
            self._cmd = self._qos = None
            self._cmd_tx_count = 0

        # Spawn effects
        self._loop.call_soon_threadsafe(effect_state, timed_out)

    def _check_buffer_for_cmd(self) -> None:
        """Check if there are commands in the queue to send."""
        # Using a simple check to avoid race conditions with lock
        if self._lock.locked():
            return

        # Acquire lock to ensure we are the only one popping from queue
        # Note: In strict asyncio, we'd use 'async with lock', but this is called
        # from call_soon_threadsafe (sync context).
        # We assume single-threaded event loop execution.

        if self._fut is not None and not self._fut.done():
            return

        try:
            *_, self._cmd, self._qos, self._fut = self._que.get_nowait()
        except Empty:
            self._cmd = self._qos = self._fut = None
            return

        self._cmd_tx_count = 0
        self._cmd_tx_limit = min(self._qos.max_retries, self.max_retry_limit) + 1

        if self._fut.done():  # e.g. TimeoutError cancelled it
            self._que.task_done()
            # Recurse/Loop to get next
            self._check_buffer_for_cmd()
            return

        try:
            assert self._cmd is not None
            self._send_cmd(self._cmd)
        finally:
            self._que.task_done()

    def _send_cmd(self, cmd: Command, is_retry: bool = False) -> None:
        """Wrapper to send a command with retries, until success or exception."""

        async def send_fnc_wrapper(kmd: Command) -> None:
            try:
                # The actual I/O call
                self._protocol.send_pkt(kmd)  # type: ignore[arg-type]
            except RamsesException as err:  # TransportError/ProtocolError
                self.set_state(IsInIdle, exception=err)

        assert cmd is not None, f"{self}: Coding error"

        try:
            self._state.cmd_sent(cmd, is_retry=is_retry)
        except ProtocolFsmError as err:
            self.set_state(IsInIdle, exception=err)
        else:
            self._loop.create_task(send_fnc_wrapper(cmd))


class _ProtocolState:
    """Base class for FSM states."""

    def __init__(self, context: ProtocolContext) -> None:
        self._context = context

        # 1. Safely get the previous state, defaulting to None if it doesn't exist yet
        old_state = getattr(context, "_state", None)

        # 2. ONLY copy variables if we are NOT in an Idle or Inactive state
        # This prevents stale data from polluting the FSM between transactions
        if old_state and not isinstance(self, Inactive | IsInIdle):
            self._sent_cmd: Command | None = old_state._sent_cmd
            self._echo_pkt: Packet | None = old_state._echo_pkt
            self._rply_pkt: Packet | None = old_state._rply_pkt
        else:
            self._sent_cmd = None
            self._echo_pkt = None
            self._rply_pkt = None

    def __repr__(self) -> str:
        msg = f"<ProtocolState state={self.__class__.__name__}"
        if self._rply_pkt:
            return msg + f" rply={self._rply_pkt._hdr}>"
        if self._echo_pkt:
            return msg + f" echo={self._echo_pkt._hdr}>"
        if self._sent_cmd:
            return msg + f" cmd_={self._sent_cmd._hdr}>"
        return msg + ">"

    def connection_made(self) -> None:
        """Do nothing, as (except for InActive) we're already connected."""
        pass

    def connection_lost(self, exc: Exception | None) -> None:
        """Transition to Inactive, regardless of current state."""
        if isinstance(self, Inactive):
            return

        if isinstance(self, IsInIdle):
            self._context.set_state(Inactive)
            return

        self._context.set_state(
            Inactive, exception=exc or ProtocolError("Connection lost")
        )

    def pkt_rcvd(self, pkt: Packet) -> None:
        """Raise a NotImplementedError."""
        raise NotImplementedError("Invalid state to receive a packet")

    def writing_paused(self) -> None:
        """Do nothing."""
        pass

    def writing_resumed(self) -> None:
        """Do nothing."""
        pass

    def cmd_sent(self, cmd: Command, is_retry: bool | None = None) -> None:
        raise ProtocolFsmError(f"Invalid state to send a command: {self._context}")


class Inactive(_ProtocolState):
    """The Protocol is not connected to the transport layer."""

    def __init__(self, context: ProtocolContext) -> None:
        super().__init__(context)
        self._sent_cmd = None
        self._echo_pkt = None

    def connection_made(self) -> None:
        """Transition to IsInIdle."""
        self._context.set_state(IsInIdle)

    def pkt_rcvd(self, pkt: Packet) -> None:
        """Raise an exception, as a packet is not expected in this state."""
        # Code._PUZZ is an internal code often used for debugging/puzzles
        if pkt.code != "7FFF":  # Code._PUZZ
            _LOGGER.warning("%s: Invalid state to receive a packet", self._context)


class IsInIdle(_ProtocolState):
    """The Protocol is not in the process of sending a Command."""

    def pkt_rcvd(self, pkt: Packet) -> None:
        """Do nothing as we're not expecting an echo, nor a reply."""
        pass

    def cmd_sent(self, cmd: Command, is_retry: bool | None = None) -> None:
        """Transition to WantEcho."""
        assert self._sent_cmd is None and is_retry is False, f"{self}: Coding error"

        self._sent_cmd = cmd
        # Hacks for specific device addressing issues would go here if needed
        self._context.set_state(WantEcho)


class WantEcho(_ProtocolState):
    """The Protocol is waiting to receive an echo Packet."""

    def __init__(self, context: ProtocolContext) -> None:
        super().__init__(context)
        self._sent_cmd = context._state._sent_cmd

    def pkt_rcvd(self, pkt: Packet) -> None:
        """If the pkt is the expected Echo, transition to IsInIdle, or WantRply."""
        assert self._sent_cmd, f"{self}: Coding error"

        # Check for matching headers (Reply arriving before Echo)
        if (
            self._sent_cmd.rx_header
            and pkt._hdr == self._sent_cmd.rx_header
            and (
                pkt.dst.id == self._sent_cmd.src.id
                # Handle HGI self-identification edge cases
                or (self._sent_cmd.src.id == "18:000730" and pkt.dst.id[:2] == "18")
            )
        ):
            # This is a Reply arriving before the Echo (race condition or fast device)
            _LOGGER.debug(
                "%s: Received reply before echo, accepting as success", self._context
            )
            self._rply_pkt = pkt
            self._context.set_state(IsInIdle, result=pkt)
            return

        is_match = pkt._hdr == self._sent_cmd.tx_header

        # Hardware swaps the generic addr0 for its own device_id.
        # If we sent from 18:000730, accept an echo from the real gateway ID.
        if not is_match and self._sent_cmd.src.id == "18:000730":
            if pkt.src.type == "18" and pkt._hdr == self._sent_cmd.tx_header.replace(
                "18:000730", pkt.src.id
            ):
                is_match = True

        if not is_match:
            return

        self._echo_pkt = pkt
        if self._sent_cmd.rx_header:
            self._context.set_state(WantRply)
        else:
            self._context.set_state(IsInIdle, result=pkt)

    def cmd_sent(self, cmd: Command, is_retry: bool | None = None) -> None:
        """Transition to WantEcho (i.e. a retransmit)."""
        assert self._sent_cmd is not None and is_retry is True, f"{self}: Coding error"
        # Logic handled by re-init of state in set_state


class WantRply(_ProtocolState):
    """The Protocol is waiting to receive a reply Packet."""

    def __init__(self, context: ProtocolContext) -> None:
        super().__init__(context)
        self._sent_cmd = context._state._sent_cmd
        self._echo_pkt = context._state._echo_pkt

    def pkt_rcvd(self, pkt: Packet) -> None:
        """If the pkt is the expected reply, transition to IsInIdle."""
        assert self._sent_cmd, f"{self}: Coding error"
        assert self._echo_pkt, f"{self}: Coding error"

        # Check for "Echo" arriving late (duplicate echo)
        # UPDATED: We now check against the echo packet's actual header instead of the command's tx_header
        if pkt._hdr == self._echo_pkt._hdr and pkt.src == self._echo_pkt.src:
            _LOGGER.debug("%s: Duplicate echo received, ignoring", self._context)
            return

        # Special handling for null log entries (Code 0418)
        if (
            self._sent_cmd.rx_header
            and self._sent_cmd.rx_header.startswith("0418|RP|")
            and self._sent_cmd.rx_header[:-2] == pkt._hdr[:-2]
        ):
            self._rply_pkt = pkt
        elif self._sent_cmd.rx_header and pkt._hdr != self._sent_cmd.rx_header:
            return
        else:
            self._rply_pkt = pkt

        self._context.set_state(IsInIdle, result=pkt)
