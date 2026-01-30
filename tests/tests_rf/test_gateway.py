#!/usr/bin/env python3
"""RAMSES RF - Unit test for Gateway."""

import asyncio
from datetime import datetime as dt
from pathlib import Path
from typing import Any

import pytest
import serial  # type: ignore[import-untyped]

from ramses_rf import Gateway
from ramses_tx import exceptions as exc


@pytest.fixture
def gwy_config() -> dict[str, Any]:
    return {}


@pytest.fixture
def gwy_dev_id() -> str:
    return "18:000730"


#
# Happy Path Tests
#
async def test_gwy_lifecycle(fake_evofw3: Gateway) -> None:
    """Test the basic lifecycle of the Gateway (Start/Stop)."""
    gwy = fake_evofw3

    # Check initial state (fixture starts it)
    assert gwy.hgi is not None
    assert gwy._protocol._is_evofw3 is True

    # Test properties
    assert gwy.status["_tx_rate"] is not None
    assert "devices" in gwy.params

    # Stop
    await gwy.stop()
    # Check that the storage worker thread has stopped
    if gwy.msg_db:
        assert gwy.msg_db._worker._thread.is_alive() is False


async def test_gwy_config(fake_evofw3: Gateway) -> None:
    """Test Gateway configuration properties."""
    gwy = fake_evofw3

    # Check config export
    conf = gwy._config
    assert "config" in conf
    assert "known_list" in conf

    assert gwy.hgi is not None  # mypy
    assert conf["_gateway_id"] == gwy.hgi.id


#
# Persistence / SQLite Tests
#
async def test_gwy_persistence_setup(fake_evofw3: Gateway, tmp_path: Path) -> None:
    """Test that the Gateway can initialize the SQLite index with a store path."""
    gwy = fake_evofw3
    store_path = tmp_path / "packet_log.db"

    # Manually trigger the creation with a store path (simulating HA config)
    # Note: typically called during gwy setup if configured
    if gwy.msg_db:
        gwy.msg_db.stop()

    gwy.create_sqlite_message_index(store=str(store_path))

    assert gwy.msg_db is not None
    assert gwy.msg_db.store == str(store_path)

    # Simulate activity
    # Use "RP" (Reply) for 3220, as " I" is not valid for this code in strict mode
    # Payload must be 5 bytes (10 hex chars) for 3220 RP: e.g. 00 (msg_id) + C0000000 (value)
    gwy.msg_db.add_record(src="01:123456", code="3220", verb="RP", payload="00C0000000")
    gwy.msg_db.flush()
    gwy.msg_db._save_db()

    assert store_path.exists()


#
# State Restoration Tests
#
async def test_gwy_state_restoration(fake_evofw3: Gateway) -> None:
    """Test packet restoration and state generation."""
    gwy = fake_evofw3

    # 1. Create a dummy packet log
    # Use a sensor (32:) for CO2 code (1298) so dispatcher accepts it
    # Use current timestamp to avoid expiration filtering by default
    now_iso = dt.now().isoformat()
    # Note: The restored packet string might have different whitespace than the input
    packet_log = {now_iso: "...  I --- 32:123456 --:------ 32:123456 1298 003 007FFF"}

    # 2. Restore packets (simulates HA startup playback)
    await gwy._restore_cached_packets(packet_log)

    # Allow a small grace period for the transport->dispatcher->DB pipeline to settle
    await asyncio.sleep(0.01)

    # 3. Verify state was updated
    state, packets = await gwy.get_state(include_expired=True)

    assert "32:123456" in gwy.device_by_id

    # Ensure the restored packet is in the output state
    # We check if '1298' (code) and '007FFF' (payload) exist in any packet string
    found = False
    for p in packets.values():
        if "1298" in p and "007FFF" in p:
            found = True
            break
    assert found, f"Packet 1298 not found in restored state: {packets}"


#
# Edge Cases
#
async def test_gwy_invalid_port() -> None:
    """Test Gateway behavior with an invalid serial port."""
    gwy = Gateway("/dev/non_existent_port")

    with pytest.raises((serial.SerialException, exc.TransportError, OSError)):
        await gwy.start()

    await gwy.stop()


async def test_gwy_double_stop(fake_evofw3: Gateway) -> None:
    """Test that calling stop() twice does not crash."""
    gwy = fake_evofw3
    await gwy.stop()
    await gwy.stop()  # Should be idempotent
