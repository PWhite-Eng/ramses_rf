"""RAMSES RF - Protocol exceptions."""

from ..exceptions import RamsesException as RamsesException


class ProtocolError(RamsesException):
    """Base class for protocol exceptions."""


class ProtocolFsmError(ProtocolError):
    """Base class for FSM exceptions."""


class ProtocolSendFailed(ProtocolFsmError):
    """The protocol failed to send a packet."""
