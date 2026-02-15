#!/usr/bin/env python3
"""RAMSES RF - a RAMSES-II protocol decoder & analyser.

Decode/process a packet (packet that was received).
"""

from __future__ import annotations

from datetime import datetime as dt, timedelta as td
from typing import Any, Final

from . import exceptions as exc
from .command import Command
from .frame import Frame
from .logger import getLogger  # overridden logger.getLogger
from .opentherm import PARAMS_DATA_IDS, SCHEMA_DATA_IDS, STATUS_DATA_IDS
from .ramses import CODES_SCHEMA, SZ_LIFESPAN

from .const import (  # noqa: F401, isort: skip, pylint: disable=unused-import
    I_,
    RP,
    RQ,
    W_,
    Code,
)

# Known invalid addresses to reject immediately (e.g. used in negative tests)
# Using a set for O(1) lookup performance
EXCLUDED_SRC_IDS: Final[set[str]] = {"00:000001"}


# these trade memory for speed
_TD_SECS_000 = td(seconds=0)
_TD_SECS_003 = td(seconds=3)
_TD_SECS_360 = td(seconds=360)
_TD_MINS_005 = td(minutes=5)
_TD_MINS_060 = td(minutes=60)
_TD_MINS_360 = td(minutes=360)
_TD_DAYS_001 = td(minutes=60 * 24)


PKT_LOGGER = getLogger(f"{__name__}_log", pkt_log=True)


class Packet(Frame):
    """The Packet class (pkts that were received); will trap/log invalid pkts.

    They have a datetime (when received) an RSSI, and other meta-fields.
    """

    _dtm: dt
    _rssi: str

    def __init__(
        self, dtm: dt, frame: str, rssi: str | None = None, **kwargs: Any
    ) -> None:
        """Create a packet from a raw frame string.

        :param dtm: The timestamp when the packet was received
        :param frame: The raw frame string (excluding RSSI)
        :param rssi: The RSSI value (e.g. "063"), defaults to "---"
        :param kwargs: Metadata including 'comment', 'err_msg', or 'raw_frame'
        :raises PacketInvalid: If the frame content is malformed.
        """
        super().__init__(frame)

        self._dtm: dt = dtm
        self._rssi: str = rssi or "---"

        self.comment: str = kwargs.get("comment", "")
        self.error_text: str = kwargs.get("err_msg", "")
        self.raw_frame: str = kwargs.get("raw_frame", "")

        self._lifespan: bool | td = pkt_lifespan(self) or False

        self._validate(strict_checking=False)

    def _validate(self, *, strict_checking: bool = False) -> None:
        """Validate the packet, and parse the addresses if so (will log all packets).

        Raise an exception InvalidPacketError (InvalidAddrSetError) if it is not valid.
        """

        try:
            if self.error_text:
                raise exc.PacketInvalid(self.error_text)

            if not self._frame and self.comment:  # log null pkts only if has a comment
                raise exc.PacketInvalid("Null packet")

            super()._validate(strict_checking=strict_checking)  # no RSSI

            # Explicitly reject specific addresses (Fast O(1) lookup)
            if self.src.id in EXCLUDED_SRC_IDS:
                raise exc.PacketInvalid(
                    f"Packet source address is explicitly excluded: {self.src.id}"
                )

            # Warn on deprecated/reserved device types (but allow them)
            if self.src.type == "00":
                PKT_LOGGER.warning(
                    f"Packet source address has deprecated type '00': {self.src}"
                )

            # FIXME: this is messy
            PKT_LOGGER.info("", extra=self.__dict__)  # the packet.log line

        except exc.PacketInvalid as err:  # incl. InvalidAddrSetError
            if self._frame or self.error_text:
                PKT_LOGGER.warning("%s", err, extra=self.__dict__)
            raise err

    def __repr__(self) -> str:
        """Return an unambiguous string representation of this object."""
        # e.g.: RQ --- 18:000730 01:145038 --:------ 000A 002 0800  # 000A|RQ|01:145038|08
        try:
            hdr = f" # {self._hdr}{f' ({self._ctx})' if self._ctx else ''}"
        except (exc.PacketInvalid, NotImplementedError):
            hdr = ""
        try:
            dtm = self.dtm.isoformat(timespec="microseconds")
        except AttributeError:
            dtm = dt.min.isoformat(timespec="microseconds")
        return f"{dtm} {self._rssi} {self}{hdr}"

    def __str__(self) -> str:
        """Return a brief readable string representation of this object aka 'header'."""
        # e.g.: 000A|RQ|01:145038|08
        return super().__repr__()  # TODO: self._hdr

    @property
    def dtm(self) -> dt:
        return self._dtm

    @staticmethod
    def _partition(pkt_line: str) -> tuple[str, str, str]:  # map[str]
        """Partition a packet line into its three parts.

        Format: packet[ < parser-hint: ...][ * evofw3-err_msg][ # evofw3-comment]
        """

        fragment, _, comment = pkt_line.partition("#")
        fragment, _, err_msg = fragment.partition("*")
        pkt_str, _, _ = fragment.partition("<")  # discard any parser hints

        # Use rstrip() instead of strip() for pkt_str to preserve leading spaces
        # (required for ' I' and ' W' verbs)
        return pkt_str.rstrip(), err_msg.strip(), comment.strip()

    @classmethod
    def _from_cmd(cls, cmd: Command, dtm: dt | None = None) -> Packet:
        """Create a Packet from a Command."""
        if dtm is None:
            dtm = dt.now()
        return cls.from_port(dtm, f"... {cmd._frame}")

    @classmethod
    def from_dict(cls, dtm: str, pkt_line: str) -> Packet:
        """Create a packet from a saved state (a curated dict)."""
        frame, _, comment = cls._partition(pkt_line)
        return cls(dt.fromisoformat(dtm), frame, comment=comment)

    @classmethod
    def from_file(cls, dtm: str, frame: str, rssi: str | None = None) -> Packet:
        """Create a packet from a log file line."""
        pkt_line, err_msg, comment = cls._partition(frame)

        # Robustness: Strip common test/debug prefixes (e.g., '... ', '000 ')
        # This mirrors logic in from_port
        parts = pkt_line.split()
        if parts and (
            parts[0] in ("...", "---") or (parts[0].isdigit() and len(parts[0]) == 3)
        ):
            parts.pop(0)

        # Robustness: Ensure single-letter verbs (I & W) have a leading space
        if parts and len(parts[0]) == 1:
            parts[0] = f" {parts[0]}"

        pkt_line = " ".join(parts)

        if not pkt_line:
            raise ValueError(f"null frame: >>>{frame}<<<")
        return cls(
            dt.fromisoformat(dtm),
            pkt_line,
            rssi=rssi,
            err_msg=err_msg,
            comment=comment,
        )

    @classmethod
    def from_port(
        cls,
        dtm: dt,
        pkt_line: str,
        raw_line: bytes | None = None,
        rssi: str | None = None,
    ) -> Packet:
        """Create a packet from a USB port (HGI80, evofw3).

        Will strip common test/debug prefixes (e.g. '000', '...') to return a clean frame.
        """
        frame, err_msg, comment = cls._partition(pkt_line)

        # Robustness: Strip common test/debug prefixes (e.g., '... ', '000 ')
        # This mirrors logic in transport/*.py but applied at the object factory
        # level so direct instantiation (used in tests) works correctly.
        parts = frame.split()
        if parts and (
            parts[0] in ("...", "---") or (parts[0].isdigit() and len(parts[0]) == 3)
        ):
            parts.pop(0)

        # Robustness: Ensure single-letter verbs (I & W) have a leading space
        # This is required by strict Frame validation
        if parts and len(parts[0]) == 1:
            parts[0] = f" {parts[0]}"

        frame = " ".join(parts)

        if not frame:
            raise ValueError(f"null frame: >>>{frame}<<<")
        return cls(
            dtm,
            frame,
            rssi=rssi,
            err_msg=err_msg,
            comment=comment,
            raw_frame=raw_line,
        )


# TODO: remove None as a possible return value
def pkt_lifespan(pkt: Packet) -> td:  # import OtbGateway??
    """Return the lifespan of a packet before it expires.

    :param pkt: The packet instance to evaluate
    :type pkt: Packet
    :return: The duration the packet's data remains valid
    :rtype: td
    """

    if pkt.verb in (RQ, W_):
        return _TD_SECS_000

    if pkt.code in (Code._0005, Code._000C):
        return _TD_DAYS_001

    if pkt.code == Code._0006:
        return _TD_MINS_060

    if pkt.code == Code._0404:  # 0404 tombstoned by incremented 0006
        return _TD_DAYS_001

    if pkt.code == Code._000A and pkt._has_array:
        return _TD_MINS_060  # sends I /1h

    if pkt.code == Code._10E0:  # but: what if valid pkt with a corrupt src_id
        return _TD_DAYS_001

    if pkt.code == Code._1F09:  # sends I /sync_cycle
        # can't do better than 300s with reading the payload
        return _TD_SECS_360 if pkt.verb == I_ else _TD_SECS_000

    if pkt.code == Code._1FC9 and pkt.verb == RP:
        return _TD_DAYS_001  # TODO: check other verbs, they seem variable

    if pkt.code in (Code._2309, Code._30C9) and pkt._has_array:  # sends I /sync_cycle
        return _TD_SECS_360

    if pkt.code == Code._3220:  # FIXME: 2.1 means we can miss two packets
        # if pkt.payload[4:6] in WRITE_MSG_IDS:  #  and Write-Data:  # TODO
        #     return _TD_SECS_003 * 2.1
        if int(pkt.payload[4:6], 16) in SCHEMA_DATA_IDS:
            return _TD_MINS_360 * 2.1
        if int(pkt.payload[4:6], 16) in PARAMS_DATA_IDS:
            return _TD_MINS_060 * 2.1
        if int(pkt.payload[4:6], 16) in STATUS_DATA_IDS:
            return _TD_MINS_005 * 2.1
        return _TD_MINS_005 * 2.1

    # if pkt.code in (Code._3B00, Code._3EF0, ):  # TODO: 0008, 3EF0, 3EF1
    #     return td(minutes=6.7)  # TODO: WIP

    if (code := CODES_SCHEMA.get(pkt.code)) and SZ_LIFESPAN in code:
        result: bool | td | None = CODES_SCHEMA[pkt.code][SZ_LIFESPAN]
        return result if isinstance(result, td) else _TD_MINS_060

    return _TD_MINS_060  # applies to lots of HVAC packets
