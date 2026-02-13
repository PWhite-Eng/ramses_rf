#!/usr/bin/env python3
"""RAMSES RF - Test the schema discovery from log files."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from ramses_rf import Gateway
from ramses_rf.helpers import shrink

from .helpers import TEST_DIR

WORK_DIR = f"{TEST_DIR}/schemas"


@pytest.mark.parametrize(
    "f_name", [f.stem for f in Path(f"{WORK_DIR}/log_files").glob("*.log")]
)
async def test_schema_discover_from_log(f_name: str) -> None:
    """Check discovery of schema from a log file."""
    path = f"{WORK_DIR}/log_files/{f_name}.log"

    # We use a context manager/finally to ensure gwy.stop() is called
    gwy = Gateway(None, input_file=path, config={})
    await gwy.start()

    # Wait for the file to be fully processed
    await gwy._protocol.wait_for_connection_lost()
    # Allow pending call_soon tasks (device/zone creation) to execute
    await asyncio.sleep(0.05)

    try:
        with open(f"{WORK_DIR}/log_files/{f_name}.json") as f:
            expected_schema = json.load(f)

        assert shrink(gwy.schema) == shrink(expected_schema)
    finally:
        await gwy.stop()


@pytest.mark.parametrize(
    "f_name", [f.stem for f in Path(f"{WORK_DIR}/jsn_files").glob("*.json")]
)
async def test_schema_load_from_json(f_name: str) -> None:
    """Check loading of schema from a json file."""
    with open(f"{WORK_DIR}/jsn_files/{f_name}.json") as f:
        schema = json.load(f)

    # We disable sending to prevent any discovery side effects during load
    gwy = Gateway(None, input_file="/dev/null", config={})
    gwy._disable_sending = True
    await gwy.start()

    try:
        # Note: load_schema is synchronous but has side effects we want to check
        from ramses_rf.schemas import load_schema

        load_schema(gwy, **schema)

        assert shrink(gwy.schema) == shrink(schema)
    finally:
        await gwy.stop()


# def test_schema_load_from_json(f_name: Path) -> None:
#     path = f"{WORK_DIR}/jsn_files/{f_name}.json"
#     gwy = Gateway(None, input_file=path, config={})  # noqa: F811

#     with open(f"{WORK_DIR}/jsn_files/{f_name}.json") as f:
#         schema = json.load(f)

#     load_schema(gwy, schema)
