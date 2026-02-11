#!/usr/bin/env python3
"""RAMSES RF - Typing for RamsesProtocol & RamsesTransport."""

from collections.abc import Callable
from typing import TYPE_CHECKING, NewType, TypeAlias, TypedDict

if TYPE_CHECKING:
    from .message import Message

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
