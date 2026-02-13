"""RAMSES RF - Protocol factory."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from ..const import DEFAULT_DISABLE_QOS, SZ_PORT_NAME
from ..logger import set_logger_timesource
from ..transports import transport_factory
from .protocol import RamsesProtocol

if TYPE_CHECKING:
    from ..transports import RamsesTransport
    from ..typing import DeviceListT, MsgHandlerT

_LOGGER = logging.getLogger(__name__)


def protocol_factory(
    msg_handler: MsgHandlerT,
    /,
    *,
    disable_qos: bool | None = DEFAULT_DISABLE_QOS,
    disable_sending: bool = False,
    enforce_include_list: bool = False,
    exclude_list: DeviceListT | None = None,
    include_list: DeviceListT | None = None,
) -> RamsesProtocol:
    """Create and return a Ramses-specific async packet Protocol."""
    return RamsesProtocol(
        msg_handler,
        disable_qos=disable_qos,
        disable_sending=disable_sending,
        enforce_include_list=enforce_include_list,
        exclude_list=exclude_list,
        include_list=include_list,
    )


async def create_stack(
    msg_handler: MsgHandlerT,
    /,
    *,
    protocol_factory_: Callable[..., RamsesProtocol] | None = None,
    transport_factory_: Callable[..., Awaitable[RamsesTransport]] | None = None,
    disable_qos: bool | None = DEFAULT_DISABLE_QOS,
    disable_sending: bool = False,
    enforce_include_list: bool = False,
    exclude_list: DeviceListT | None = None,
    include_list: DeviceListT | None = None,
    **kwargs: Any,
) -> tuple[RamsesProtocol, RamsesTransport]:
    """Utility function to provide a Protocol / Transport pair.

    Architecture: gwy (client) -> msg (Protocol) -> pkt (Transport) -> HGI/log (or dict)
    """
    # Identify if read-only (packet log/dict) implies disable_sending
    read_only = kwargs.get("packet_dict") or kwargs.get("packet_log")
    disable_sending = disable_sending or bool(read_only)

    p_factory = protocol_factory_ or protocol_factory
    t_factory = transport_factory_ or transport_factory

    protocol = p_factory(
        msg_handler,
        disable_qos=disable_qos,
        disable_sending=disable_sending,
        enforce_include_list=enforce_include_list,
        exclude_list=exclude_list,
        include_list=include_list,
    )

    # Cast to Any to bypass type check against stale Transport signatures
    transport = await t_factory(
        cast(Any, protocol),
        disable_sending=disable_sending,
        **kwargs,
    )

    # If processing a packet log (no serial port), set the time source to the packet timestamps
    # rather than the system clock.
    if not kwargs.get(SZ_PORT_NAME):
        # We assume the transport has this method if it's a file transport
        # (This preserves logic from the original protocol.py)
        if hasattr(transport, "_dt_now"):
            set_logger_timesource(transport._dt_now)
            _LOGGER.warning(
                "Logger datetimes maintained as most recent packet timestamp"
            )

    return protocol, transport
