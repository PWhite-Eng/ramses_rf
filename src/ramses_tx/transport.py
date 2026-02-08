#!/usr/bin/env python3
"""RAMSES RF - RAMSES-II compatible packet transport.

Operates at the pkt layer of: app - msg - pkt - h/w

For ser2net, use the following YAML with: ``ser2net -c misc/ser2net.yaml``

.. code-block::

    connection: &con00
    accepter: telnet(rfc2217),tcp,5001
    timeout: 0
    connector: serialdev,/dev/ttyUSB0,115200n81,local
    options:
        max-connections: 3

For ``socat``, see:

.. code-block::

    socat -dd pty,raw,echo=0 pty,raw,echo=0
    python client.py monitor /dev/pts/0
    cat packet.log | cut -d ' ' -f 2- | unix2dos > /dev/pts/1

For re-flashing evofw3 via Arduino IDE on *my* atmega328p (YMMV):

  - Board:      atmega328p (SW UART)
  - Bootloader: Old Bootloader
  - Processor:  atmega328p (5V, 16 MHz)
  - Host:       57600 (or 115200, YMMV)
  - Pinout:     Nano

For re-flashing evofw3 via Arduino IDE on *my* atmega32u4 (YMMV):

  - Board:      atmega32u4 (HW UART)
  - Processor:  atmega32u4 (5V, 16 MHz)
  - Pinout:     Pro Micro

RAMSES RF - The Transport layer (shim for backward compatibility).

This module is deprecated. Please import from `ramses_tx.transports` instead.
"""

from __future__ import annotations

from .const import SZ_READER_TASK  # stored in const.py, but exposed here for compat
from .transports import (
    CallbackTransport,
    FileTransport,
    MqttTransport,
    PortTransport,
    RamsesTransportT,
    is_hgi80,
    transport_factory,
)

__all__ = [
    "SZ_READER_TASK",
    "CallbackTransport",
    "FileTransport",
    "MqttTransport",
    "PortTransport",
    "RamsesTransportT",
    "is_hgi80",
    "transport_factory",
]
