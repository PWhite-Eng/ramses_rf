#!/usr/bin/env python3
"""RAMSES RF - Typing for RamsesProtocol & RamsesTransport."""

from collections.abc import Callable
from typing import TYPE_CHECKING, NewType, TypeAlias, TypedDict, TypeVar

if TYPE_CHECKING:
    from .message import Message
    from .transports.base import RamsesTransport

MsgFilterT = Callable[["Message"], bool]
MsgHandlerT = Callable[["Message"], None]
SerPortNameT = str

# Strict Types
DeviceIdT = NewType("DeviceIdT", str)  # TypeVar('DeviceIdT', bound=str)
DevIndexT = NewType("DevIndexT", str)

DeviceTraitsT = TypedDict(
    "DeviceTraitsT",
    {
        "alias": str | None,
        "faked": bool | None,
        "class": str | None,
    },
)

DeviceListT: TypeAlias = dict[DeviceIdT, DeviceTraitsT]

RamsesTransportT = TypeVar("RamsesTransportT", bound="RamsesTransport")


class PortConfigT(TypedDict, total=False):
    """
    Type definition for serial port configuration.

    :param baudrate: The communication speed (e.g., 57600, 115200).
    :param dsrdtr: Enable DSR/DTR flow control.
    :param rtscts: Enable RTS/CTS flow control.
    :param timeout: Read timeout in seconds.
    :param xonxoff: Enable software flow control.
    """

    baudrate: int
    dsrdtr: bool
    rtscts: bool
    timeout: int
    xonxoff: bool


class PktLogConfigT(TypedDict, total=False):
    """
    Type definition for packet logging configuration.

    :param file_name: The name of the log file.
    :param rotate_backups: The number of backup files to keep.
    :param rotate_bytes: The maximum file size in bytes before rotation, or None to disable.
    """

    file_name: str
    rotate_backups: int
    rotate_bytes: int | None
