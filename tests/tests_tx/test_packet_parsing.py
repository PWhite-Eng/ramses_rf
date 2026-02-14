"""Test the parsing logic of Frame and Packet classes specifically."""

import re
from datetime import datetime as dt

import pytest

from ramses_tx import exceptions as exc
from ramses_tx.const import I_, RP, RQ, W_
from ramses_tx.frame import Frame
from ramses_tx.packet import Packet

# This mimics the Regex in base.py/const.py
PKT_LINE_REGEX = re.compile(
    r"^(?P<dtm>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6})?\s*"
    r"(?P<rssi>\d{3}|---)?\s*"
    r"(?P<pkt>.*)$"
)


def normalize_like_base(pkt_str: str) -> str:
    """Mimic the exact normalization logic currently in base.py."""
    # 1. Normalize internal spaces
    pkt_parts = pkt_str.split()

    # 2. Restore leading space for single-letter verbs
    if pkt_parts and len(pkt_parts[0]) == 1:
        pkt_parts[0] = f" {pkt_parts[0]}"

    pkt = " ".join(pkt_parts)

    # 3. Ensure trailing space for empty payloads (length 000)
    if pkt_parts and pkt_parts[-1] == "000":
        pkt += " "

    return pkt


def test_frame_parsing_standard() -> None:
    """Test parsing a standard packet (I verb)."""
    # Input from base.py (after stripping RSSI) for " I --- 01:123456 ..."
    raw_frame = " I --- 01:123456 --:------ 01:123456 000A 002 0102"

    f = Frame(raw_frame)
    assert f.verb == I_
    assert f.seqn == "---"
    assert f.src.id == "01:123456"
    assert f.code == "000A"
    assert f.len_ == "002"
    assert f.payload == "0102"


def test_frame_parsing_empty_payload() -> None:
    """Test parsing a packet with length 000 (Empty Payload)."""
    # This is the critical regression case.

    # CASE 1: No trailing space -> Fails in Legacy Frame (split(" ") produces 7 fields)
    raw_no_space = " I --- 01:123456 --:------ 01:123456 000A 000"
    with pytest.raises(exc.PacketInvalid) as e:
        Frame(raw_no_space)
    assert "invalid structure" in str(e.value)

    # CASE 2: With trailing space -> Should Pass in Legacy Frame
    raw_with_space = " I --- 01:123456 --:------ 01:123456 000A 000 "
    f = Frame(raw_with_space)
    assert f.len_ == "000"
    assert f.payload == ""


def test_frame_parsing_verbs() -> None:
    """Test parsing logic for different Verbs (RP, W, RQ)."""
    # Test RP (Response) - Zone Temp
    raw_rp = "RP --- 01:123456 18:000730 --:------ 30C9 003 000798"
    f = Frame(raw_rp)
    assert f.verb == RP
    assert f.code == "30C9"
    assert f.len_ == "003"
    assert f.payload == "000798"

    # Test W (Write) - Zone Setpoint (single letter verb logic check)
    # Note: " W" must have a leading space handled by normalizer
    raw_w = " W --- 01:123456 --:------ 01:123456 2309 003 0107D0"
    f = Frame(raw_w)
    assert f.verb == W_
    assert f.code == "2309"
    assert f.len_ == "003"
    assert f.payload == "0107D0"

    # Test RQ (Request)
    raw_rq = "RQ --- 18:000730 01:123456 --:------ 30C9 001 01"
    f = Frame(raw_rq)
    assert f.verb == RQ
    assert f.code == "30C9"


def test_packet_class_behavior() -> None:
    """Test how Packet() class handles the input strings."""
    dtm = dt.now()

    # 1. Standard
    base_str = "000  I --- 01:123456 --:------ 01:123456 000A 002 0102"
    p = Packet(dtm, base_str)
    assert p.verb == I_

    # 2. Empty Payload (normalized by base.py to have trailing space)
    # 000 + space + I ... 000 + space
    base_str_empty = "000  I --- 01:123456 --:------ 01:123456 000A 000 "
    p = Packet(dtm, base_str_empty)
    assert p.len_ == "000"
    assert p.payload == ""


def test_base_normalization_logic() -> None:
    """Test if our base.py normalization correctly adds the space."""
    # Input from log file (comments stripped)
    log_line_content = " I --- 01:000001 --:------ 01:000001 000A 000"

    # Run normalization
    normalized = normalize_like_base(log_line_content)

    # Check if trailing space was added
    assert normalized.endswith(" 000 ")
    assert len(normalized.split(" ")) == 8  # I, ---, addr, addr, addr, code, len, ''


def test_full_chain_simulation() -> None:
    """Simulate File -> Base -> Packet -> Frame chain."""
    line = (
        "2026-02-07T12:00:00.000000 045  I --- 01:123456 --:------ 01:123456 000A 000"
    )
    dtm_str = "2026-02-07T12:00:00.000000"
    # convert ISO 8601 formatted string into a datetime object
    dtm_dt = dt.fromisoformat(dtm_str)

    # 1. File.py stripping
    line = line.strip()

    # 2. Base.py Regex
    match = PKT_LINE_REGEX.match(line)
    assert match is not None
    rssi = match.group("rssi") or "---"  # "045"
    pkt_raw = match.group("pkt")  # " I --- ... 000A 000"

    # 3. Base.py Normalization
    pkt_norm = normalize_like_base(pkt_raw)

    # 4. Base.py Reconstruction
    frame_str = f"{rssi} {pkt_norm}"  # "045  I ... 000 "

    # 5. Packet init
    # Packet slices [4:] -> " I ... 000 "
    p = Packet(dtm_dt, frame_str)

    assert p.len_ == "000"
    assert p.payload == ""


def test_rq_double_space_issue() -> None:
    """Test the 'Invalid address set' error for RQ packets."""
    dtm = dt.now()
    # Log line might have double spaces for alignment
    # "045 RQ --- 21:000 21:000 --:-- ..."

    pkt_raw = "RQ --- 21:111111 21:111111 --:------ 000A 002 0102"

    # Normalization should ensure single spaces
    pkt_norm = normalize_like_base(pkt_raw)

    rssi = "045"
    frame_str = f"{rssi} {pkt_norm}"

    p = Packet(dtm, frame_str)
    # If this raises PacketInvalid(addr set), it's the logic inside Frame
    assert p.verb == RQ
    assert p.src.id == "21:111111"
    assert p.dst.id == "21:111111"
