#!/usr/bin/env python3
"""RAMSES RF - Reliability and Stress tests for Gateway persistence."""

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from ramses_rf import Gateway


@pytest.fixture
def gwy_config() -> dict[str, Any]:
    return {}


@pytest.fixture
def gwy_dev_id() -> str:
    return "18:000730"


async def test_database_stress_test(fake_evofw3: Gateway, tmp_path: Path) -> None:
    """Test that the database handles high-volume writes without blocking or crashing."""
    gwy = fake_evofw3
    store_path = tmp_path / "stress_test.db"

    # Setup DB
    if gwy.msg_db:
        gwy.msg_db.stop()
    gwy.create_sqlite_message_index(store=str(store_path))
    assert gwy.msg_db is not None

    # Generate 1,000 dummy records rapidly using known valid packets
    count = 1000

    ctl_id = "18:000730"
    sensor_id = "01:123456"

    for i in range(count):
        scenario = i % 3

        if scenario == 0:
            # 3220 (OpenTherm) - Keep STATIC to avoid parity errors.
            # These will be deduplicated by the DB (INSERT OR REPLACE), testing update logic.
            code = "3220"
            verb = "RP"
            src = ctl_id
            payload = "00C0000000"  # Valid static payload

        elif scenario == 1:
            # 3150 (Heat Demand) - Simple 2-byte payload, no parity check.
            # We make this UNIQUE to fill the DB row count.
            code = "3150"
            verb = " I"
            src = sensor_id
            # Payload: 00 + (i mod 100) as hex. e.g. 0001, 0002...
            payload = f"00{i % 200:02X}"  # Limit to valid range 0-200 just in case

        else:
            # 1298 (CO2/Generic) - 3 bytes. No complex validation.
            # Make this the primary UNIQUE generator.
            code = "1298"
            verb = " I"
            src = sensor_id
            # Payload: 00 + i as hex. e.g. 000001, 0003E8...
            payload = f"00{i:04X}"

        gwy.msg_db.add_record(src=src, code=code, verb=verb, payload=payload)

    # Force a flush to disk
    # This waits for the worker thread to process the queue
    await asyncio.get_running_loop().run_in_executor(None, gwy.msg_db.flush)

    # Trigger snapshot to disk
    await asyncio.get_running_loop().run_in_executor(None, gwy.msg_db._save_db)

    # Now verify the file on disk
    conn = sqlite3.connect(store_path)
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM messages")
    row = cursor.fetchone()
    conn.close()

    saved_count = row[0]

    # We expect:
    # - 1 record for Scenario 0 (deduplicated)
    # - ~200 records for Scenario 1 (cycles 0-199)
    # - ~333 records for Scenario 2 (unique payloads)
    # Total should be well over 500
    assert saved_count > 500, f"Expected >500 records, found {saved_count}"


async def test_database_corruption_resilience(
    fake_evofw3: Gateway, tmp_path: Path
) -> None:
    """Test that the Gateway starts even if the database file is corrupted."""
    gwy = fake_evofw3
    store_path = tmp_path / "corrupt.db"

    # 1. Create a corrupt file (just random text)
    with open(store_path, "w") as f:
        f.write("This is not a SQLite database.")

    # 2. Try to start the MessageIndex with this file
    # It should NOT raise an exception, but likely log an error and start empty
    if gwy.msg_db:
        gwy.msg_db.stop()

    # This should be safe
    gwy.create_sqlite_message_index(store=str(store_path))

    assert gwy.msg_db is not None
    # Ideally, it should have backed up the bad file or just ignored it.
    # For now, we assert it didn't crash and is usable.

    # 3. Verify we can still use it (it likely initialized a new memory DB)
    gwy.msg_db.add_record(src="01:123456", code="3220", verb="RP", payload="00C0000000")
    gwy.msg_db.flush()

    assert len(gwy.msg_db.msgs) == 1
