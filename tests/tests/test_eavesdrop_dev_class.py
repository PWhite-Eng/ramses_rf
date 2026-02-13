#!/usr/bin/env python3
"""RAMSES RF - Test eavesdropping of device class."""

import asyncio
import contextlib
import json
from dataclasses import replace
from pathlib import Path, PurePath

import pytest

from ramses_rf import Gateway
from ramses_tx import exceptions as exc
from ramses_tx.message import Message
from ramses_tx.packet import Packet

from .helpers import TEST_DIR, assert_expected

WORK_DIR = f"{TEST_DIR}/eavesdrop_dev_class"


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    def id_fnc(param: Path) -> str:
        return PurePath(param).name

    folders = [f for f in Path(WORK_DIR).iterdir() if f.is_dir() and f.name[:1] != "_"]
    metafunc.parametrize("dir_name", folders, ids=id_fnc)


def test_packets_from_log_file(dir_name: Path) -> None:
    """Check if all packets are parsed correctly."""

    def proc_log_line(msg: Message) -> None:
        assert msg.src._SLUG in eval(msg._pkt.comment)

    path = f"{dir_name}/packet.log"

    gwy = Gateway(None, input_file=path, config={"enable_eavesdrop": False})
    gwy.config = replace(
        gwy.config, enable_eavesdrop=True
    )  # Test setting this config attr

    gwy.add_msg_handler(proc_log_line)

    try:
        await gwy.start()
    finally:
        await gwy.stop()


# duplicate in test_eavesdrop_schema
async def test_dev_eavesdrop_on_(dir_name: Path) -> None:
    """Check discovery of schema and known_list *with* eavesdropping."""

    path = f"{dir_name}/packet.log"
    gwy = Gateway(None, input_file=path, config={"enable_eavesdrop": True})
    await gwy.start()

    # Wait for the file to be fully processed
    await gwy._protocol.wait_for_connection_lost()
    await asyncio.sleep(0.05)  # Allow pending tasks to complete

    try:
        with open(f"{dir_name}/known_list_eavesdrop_on.json") as f:
            assert_expected(gwy.known_list, json.load(f).get("known_list"))
    finally:
        await gwy.stop()


async def test_dev_eavesdrop_off(dir_name: Path) -> None:
    """Check discovery of schema and known_list *without* eavesdropping."""

    path = f"{dir_name}/packet.log"
    gwy = Gateway(None, input_file=path, config={"enable_eavesdrop": False})
    await gwy.start()

    # Wait for the file to be fully processed
    await gwy._protocol.wait_for_connection_lost()
    await asyncio.sleep(0.05)  # Allow pending tasks to complete

    try:
        with open(f"{dir_name}/known_list_eavesdrop_off.json") as f:
            assert_expected(gwy.known_list, json.load(f).get("known_list"))
    finally:
        await gwy.stop()
