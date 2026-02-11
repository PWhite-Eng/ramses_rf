#!/usr/bin/env python3
"""RAMSES RF - a RAMSES-II protocol decoder & analyser.
`ramses_tx` takes care of the RF protocol (lower) layer.
"""

from __future__ import annotations

import asyncio
from functools import partial
from logging import Logger
from logging.handlers import QueueListener
from typing import Any

from .address import (
    ALL_DEV_ADDR,
    ALL_DEVICE_ID,
    NON_DEV_ADDR,
    NON_DEVICE_ID,
    Address,
    is_valid_dev_id,
)
from .command import CODE_API_MAP, Command
from .const import (
    DEV_ROLE_MAP,
    DEV_TYPE_MAP,
    F9,
    FA,
    FC,
    FF,
    I_,
    RP,
    RQ,
    SZ_ACTIVE_HGI,
    SZ_BOUND_TO,
    SZ_DEVICE_ROLE,
    SZ_DOMAIN_ID,
    SZ_SERIAL_PORT,
    SZ_ZONE_CLASS,
    SZ_ZONE_IDX,
    SZ_ZONE_MASK,
    SZ_ZONE_TYPE,
    W_,
    ZON_ROLE_MAP,
    Code,
    DevRole,
    DevType,
    IndexT,
    Priority,
    VerbT,
    ZoneRole,
)
from .gateway import Engine
from .logger import set_pkt_logging
from .message import Message
from .models import QosParams
from .packet import PKT_LOGGER, Packet
from .protocol import RamsesProtocol, protocol_factory
from .ramses import (
    _2411_PARAMS_SCHEMA,
    CODES_BY_DEV_SLUG,
    CODES_SCHEMA,
    SZ_DATA_TYPE,
    SZ_DATA_UNIT,
    SZ_DESCRIPTION,
    SZ_MAX_VALUE,
    SZ_MIN_VALUE,
    SZ_PRECISION,
)
from .transports import (
    FileTransport,
    PortTransport,
    RamsesTransport,
    is_hgi80,
    transport_factory,
)
from .typing import DeviceIdT, DeviceListT
from .version import VERSION

__all__ = [
    "VERSION",
    "Engine",
    #
    "SZ_ACTIVE_HGI",
    "SZ_DEVICE_ROLE",
    "SZ_DOMAIN_ID",
    "SZ_SERIAL_PORT",
    "SZ_ZONE_CLASS",
    "SZ_ZONE_IDX",
    "SZ_ZONE_MASK",
    "SZ_ZONE_TYPE",
    "SZ_BOUND_TO",
    # Schema-related constants
    "SZ_DATA_UNIT",
    "SZ_DESCRIPTION",
    "SZ_DATA_TYPE",
    "SZ_MAX_VALUE",
    "SZ_MIN_VALUE",
    "SZ_PRECISION",
    "_2411_PARAMS_SCHEMA",
    #
    "ALL_DEV_ADDR",
    "ALL_DEVICE_ID",
    "NON_DEV_ADDR",
    "NON_DEVICE_ID",
    #
    "CODE_API_MAP",
    "CODES_BY_DEV_SLUG",  # shouldn't export this
    "CODES_SCHEMA",
    "DEV_ROLE_MAP",
    "DEV_TYPE_MAP",
    "ZON_ROLE_MAP",
    #
    "I_",
    "RP",
    "RQ",
    "W_",
    "F9",
    "FA",
    "FC",
    "FF",
    #
    "DeviceIdT",
    "DeviceListT",
    "DevRole",
    "DevType",
    "IndexT",
    "VerbT",
    "ZoneRole",
    #
    "Address",
    "Code",
    "Command",
    "Message",
    "Packet",
    "Priority",
    "QosParams",
    #
    "RamsesProtocol",
    "extract_known_hgi_id",
    "protocol_factory",
    #
    "FileTransport",
    "PortTransport",
    "RamsesTransport",
    "is_hgi80",
    "transport_factory",
    #
    "is_valid_dev_id",
    "set_pkt_logging_config",
]


async def set_pkt_logging_config(**config: Any) -> tuple[Logger, QueueListener | None]:
    """
    Set up ramses packet logging to a file or port.
    Must run async in executor to prevent HA blocking call opening packet log file.

    :param config: if file_name is included, opens packet_log file
    :return: a tuple (logging.Logger, QueueListener)
    """
    loop = asyncio.get_running_loop()
    listener = await loop.run_in_executor(
        None, partial(set_pkt_logging, PKT_LOGGER, **config)
    )
    return PKT_LOGGER, listener


def extract_known_hgi_id(
    include_list: DeviceListT,
    /,
    *,
    disable_warnings: bool = False,
    strict_checking: bool = False,
) -> DeviceIdT | None:
    """Return the device_id of the gateway specified in the include_list."""
    return RamsesProtocol._extract_known_hgi_id(
        include_list,
        disable_warnings=disable_warnings,
        strict_checking=strict_checking,
    )
