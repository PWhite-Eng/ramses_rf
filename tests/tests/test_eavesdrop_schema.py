#!/usr/bin/env python3
"""RAMSES RF - Test eavesdropping of the system schema."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path, PurePath
from typing import Any

import pytest

from ramses_rf import Gateway

from .helpers import TEST_DIR, assert_expected

WORK_DIR = f"{TEST_DIR}/eavesdrop_schema"


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    def id_fnc(param: Path) -> str:
        return PurePath(param).name

    folders = [f for f in Path(WORK_DIR).iterdir() if f.is_dir() and f.name[:1] != "_"]
    metafunc.parametrize("dir_name", folders, ids=id_fnc)


async def assert_schemas_equal(gwy: Gateway, expected_schema: dict[str, Any]) -> None:
    """Check the gateway schema and the expected schema are equal."""
    schema, packets = await gwy.get_state(include_expired=True)
    assert_expected(schema, expected_schema)


async def test_eavesdrop_off(dir_name: Path) -> None:
    """Check discovery of schema and known_list *without* eavesdropping."""

    path = f"{dir_name}/packet.log"
    gwy = Gateway(None, input_file=path, config={"enable_eavesdrop": False})
    await gwy.start()

    # Wait for the file to be fully processed
    await gwy._protocol.wait_for_connection_lost()
    await asyncio.sleep(0.05)  # Allow pending tasks to complete

    try:
        with open(f"{dir_name}/schema_eavesdrop_off.json") as f:
            await assert_schemas_equal(gwy, json.load(f))
    finally:
        await gwy.stop()


async def test_eavesdrop_on_(dir_name: Path) -> None:
    """Check discovery of schema and known_list *with* eavesdropping."""

    path = f"{dir_name}/packet.log"
    gwy = Gateway(None, input_file=path, config={"enable_eavesdrop": True})
    await gwy.start()

    # Wait for the file to be fully processed
    await gwy._protocol.wait_for_connection_lost()
    await asyncio.sleep(0.05)  # Allow pending tasks to complete

    try:
        with open(f"{dir_name}/schema_eavesdrop_on.json") as f:
            await assert_schemas_equal(gwy, json.load(f))
    finally:
        await gwy.stop()
