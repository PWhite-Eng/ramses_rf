"""RAMSES RF - Shared data models and parameters."""

from dataclasses import dataclass, field
from datetime import datetime as dt

from .const import (
    DEFAULT_GAP_DURATION,
    DEFAULT_MAX_RETRIES,
    DEFAULT_NUM_REPEATS,
    DEFAULT_SEND_TIMEOUT,
    DEFAULT_WAIT_FOR_REPLY,
    Priority,
)
from .packet import Packet


@dataclass
class QosParams:
    """A container for QoS attributes and state."""

    max_retries: int = DEFAULT_MAX_RETRIES
    timeout: float = DEFAULT_SEND_TIMEOUT
    wait_for_reply: bool | None = DEFAULT_WAIT_FOR_REPLY

    # Internal state tracking (not set during instantiation)
    _echo_pkt: Packet | None = field(default=None, init=False)
    _rply_pkt: Packet | None = field(default=None, init=False)

    _dt_cmd_sent: dt | None = field(default=None, init=False)
    _dt_echo_rcvd: dt | None = field(default=None, init=False)
    _dt_rply_rcvd: dt | None = field(default=None, init=False)


@dataclass
class SendParams:
    """A container for Send attributes and state."""

    gap_duration: float = DEFAULT_GAP_DURATION
    num_repeats: int = DEFAULT_NUM_REPEATS
    priority: Priority = Priority.DEFAULT

    # Internal state tracking (not set during instantiation)
    _dt_cmd_arrived: dt | None = field(default=None, init=False)
    _dt_cmd_queued: dt | None = field(default=None, init=False)
    _dt_cmd_sent: dt | None = field(default=None, init=False)
