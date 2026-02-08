"""RAMSES RF - Transports package."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeAlias

from .. import exceptions as exc
from ..const import DEFAULT_TIMEOUT_MQTT, DEFAULT_TIMEOUT_PORT
from .file import FileTransport
from .ip import CallbackTransport
from .mqtt import MqttTransport
from .serial import PortTransport, create_serial_port, is_hgi80

if TYPE_CHECKING:
    from ..protocol import RamsesProtocolT
    from ..schemas import PortConfigT
    from ..typing import SerPortNameT


_LOGGER = logging.getLogger(__name__)

RamsesTransportT: TypeAlias = (
    FileTransport | MqttTransport | PortTransport | CallbackTransport
)

__all__ = [
    "CallbackTransport",
    "FileTransport",
    "MqttTransport",
    "PortTransport",
    "RamsesTransportT",
    "is_hgi80",
    "transport_factory",
]


async def transport_factory(
    protocol: RamsesProtocolT,
    /,
    *,
    port_name: SerPortNameT | None = None,
    port_config: PortConfigT | None = None,
    packet_log: str | None = None,
    packet_dict: dict[str, str] | None = None,
    transport_constructor: Callable[..., Awaitable[RamsesTransportT]] | None = None,
    disable_sending: bool = False,
    extra: dict[str, Any] | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
    log_all: bool = False,
    **kwargs: Any,
) -> RamsesTransportT:
    """Create and return a Ramses-specific async packet Transport."""

    # Extract autostart (default to False if missing), used in transport_constructor only
    autostart = kwargs.pop("autostart", False)

    # If a constructor is provided, delegate entirely to it.
    if transport_constructor:
        _LOGGER.debug("transport_factory: Delegating to external transport_constructor")
        return await transport_constructor(
            protocol,
            disable_sending=disable_sending,
            extra=extra,
            autostart=autostart,
            **kwargs,
        )

    if len([x for x in (packet_dict, packet_log, port_name) if x is not None]) != 1:
        _LOGGER.warning(
            f"Input: packet_dict: {packet_dict}, packet_log: {packet_log}, port_name: {port_name}"
        )
        raise exc.TransportSourceInvalid(
            "Packet source must be exactly one of: packet_dict, packet_log, port_name"
        )

    # File
    if (pkt_source := packet_log or packet_dict) is not None:
        return FileTransport(pkt_source, protocol, extra=extra, loop=loop, **kwargs)

    assert port_name is not None  # mypy check
    assert port_config is not None  # mypy check

    # MQTT
    if port_name[:4] == "mqtt":
        # Check for custom timeout in kwargs, fallback to constant
        mqtt_timeout = kwargs.get("timeout", DEFAULT_TIMEOUT_MQTT)

        mqtt_transport = MqttTransport(
            port_name,
            protocol,
            disable_sending=bool(disable_sending),
            extra=extra,
            loop=loop,
            log_all=log_all,
            **kwargs,
        )

        try:
            await protocol.wait_for_connection_made(timeout=mqtt_timeout)
        except Exception:
            mqtt_transport.close()
            raise

        return mqtt_transport

    # Serial
    ser_instance = create_serial_port(port_name, port_config)

    ser_transport = PortTransport(
        ser_instance,
        protocol,
        disable_sending=bool(disable_sending),
        extra=extra,
        loop=loop,
        **kwargs,
    )

    await protocol.wait_for_connection_made(timeout=DEFAULT_TIMEOUT_PORT)
    return ser_transport
