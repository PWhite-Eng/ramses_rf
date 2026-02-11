"""RAMSES RF - Protocol interfaces."""

from typing import Protocol

from ..packet import Packet


class IPacketSender(Protocol):
    """Interface for an entity that can send packets (e.g. RamsesProtocol)."""

    def send_pkt(self, pkt: Packet) -> None:
        """Send a packet to the transport."""
        ...

    def connection_lost(self, exc: Exception | None) -> None:
        """Notify that the connection was lost."""
        ...
