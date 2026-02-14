"""Test the parsing logic of Frame and Packet classes specifically."""

import re
from datetime import datetime as dt

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
    """Mimic the exact normalization logic currently in base.py.

    Updated to reflect the removal of the 'force trailing space' hack,
    as Frame() is now robust enough to handle it.
    """
    # 1. Normalize internal spaces (removes double spaces)
    pkt_parts = pkt_str.split()

    # 2. Restore leading space for single-letter verbs (I, W)
    if pkt_parts and len(pkt_parts[0]) == 1:
        pkt_parts[0] = f" {pkt_parts[0]}"

    pkt = " ".join(pkt_parts)

    return pkt


def test_frame_parsing_standard() -> None:
    """Test parsing a standard packet (I verb)."""
    # Input expected by Frame is now just the protocol string
    # (RSSI is handled by Packet, not Frame)
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
    # This is the critical regression fix verification.

    # CASE 1: No trailing space ->
    # PREVIOUSLY: Failed in Legacy Frame (split(" ") produced 7 fields)
    # NOW: Should PASS because Frame safely handles missing payload field
    raw_no_space = " I --- 01:123456 --:------ 01:123456 000A 000"

    f = Frame(raw_no_space)
    assert f.len_ == "000"
    assert f.payload == ""

    # CASE 2: With trailing space -> Should still Pass
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
    # NOTE: Packet() now accepts RSSI as a named argument,
    # and the frame string must NOT contain the RSSI prefix.
    frame_str = " I --- 01:123456 --:------ 01:123456 000A 002 0102"
    rssi_val = "000"

    p = Packet(dtm, frame_str, rssi=rssi_val)
    assert p.verb == I_
    assert p._rssi == "000"

    # 2. Empty Payload (without trailing space)
    # The new Packet/Frame classes should handle this cleanly without help.
    frame_str_empty = " I --- 01:123456 --:------ 01:123456 000A 000"

    p = Packet(dtm, frame_str_empty, rssi=rssi_val)
    assert p.len_ == "000"
    assert p.payload == ""


def test_base_normalization_logic() -> None:
    """Test if our base.py normalization correctly handles spacing."""
    # Input from log file (comments stripped)
    log_line_content = " I --- 01:000001 --:------ 01:000001 000A 000"

    # Run normalization
    normalized = normalize_like_base(log_line_content)

    # Check that it DOES NOT enforce a trailing space anymore
    # But it DOES enforce a leading space for 'I'
    assert normalized == " I --- 01:000001 --:------ 01:000001 000A 000"
    assert normalized.startswith(" ")


def test_full_chain_simulation() -> None:
    """Simulate File -> Base -> Packet -> Frame chain."""
    # This line has an RSSI of 045
    line = (
        "2026-02-07T12:00:00.000000 045  I --- 01:123456 --:------ 01:123456 000A 000"
    )
    dtm_str = "2026-02-07T12:00:00.000000"
    dtm_dt = dt.fromisoformat(dtm_str)

    # 1. File.py stripping
    line = line.strip()

    # 2. Base.py Regex (Extract RSSI vs Packet)
    match = PKT_LINE_REGEX.match(line)
    assert match is not None
    rssi = match.group("rssi") or "---"  # "045"
    pkt_raw = match.group("pkt")  # " I --- ... 000A 000"

    # 3. Base.py Normalization
    pkt_norm = normalize_like_base(pkt_raw)

    # 4. Packet init
    # We no longer concat RSSI + Frame. We pass them separately.
    p = Packet(dtm_dt, pkt_norm, rssi=rssi)

    assert p.len_ == "000"
    assert p.payload == ""
    assert p._rssi == "045"

    # Verify repr representation includes RSSI (str(p) only shows the protocol frame)
    assert "045" in repr(p)


def test_rq_double_space_issue() -> None:
    """Test parsing logic when input has double spaces."""
    dtm = dt.now()

    # Raw string simulating a log file with extra spaces
    # Changed addresses to distinct values (Src != Dst) to pass validation
    pkt_raw = "RQ ---  21:111111   01:222222 --:------ 000A 002 0102"

    # Normalization should ensure single spaces
    pkt_norm = normalize_like_base(pkt_raw)

    # Should result in clean single spaces
    assert "  " not in pkt_norm

    rssi = "045"

    p = Packet(dtm, pkt_norm, rssi=rssi)

    assert p.verb == RQ
    assert p.src.id == "21:111111"
    assert p.dst.id == "01:222222"
