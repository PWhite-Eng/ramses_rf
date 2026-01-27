"""
Benchmark: Storage Architectures for ramses_rf
Usage: pytest tests/benchmarks/test_db_storage.py --benchmark-columns=min,mean,max,ops
"""

import sqlite3
from collections import OrderedDict
from datetime import datetime as dt, timedelta as td
from typing import Any

import orjson
import pytest

# Import internal classes
from ramses_rf.database import MessageIndex
from ramses_tx.message import Message
from ramses_tx.packet import Packet

# --- MOCKS & UTILS ----------------------------------------------------------


def create_dummy_messages(count: int = 1000) -> list[Message]:
    """Generate a list of valid Message objects for testing."""
    msgs = []
    base_time = dt.fromisoformat("2025-01-01T12:00:00.000000")

    # We use a standard payload (I|000A) to ensure parsing occurs
    for i in range(count):
        ts = base_time + td(milliseconds=i)

        # FIX: Ensure 'i' wraps around 255 so hex length stays exactly 2 chars
        # FIX: 'I' and 'W' verbs require a leading space in the frame structure
        # '000  I' (2 spaces) ensures that stripping 4 chars leaves ' I ...'
        safe_i = i % 256
        pkt_str = (
            f"000  I --- 01:123456 --:------ 01:123456 000A 006 0012345678{safe_i:02X}"
        )

        pkt = Packet.from_port(ts, pkt_str)
        msgs.append(Message(pkt))
    return msgs


# --- ARCHITECTURES ----------------------------------------------------------


class ArchBaselineDict:
    """The 'Original': Pure in-memory dictionary."""

    def __init__(self) -> None:
        self._msgs: OrderedDict[str, Message] = OrderedDict()

    def add(self, msg: Message) -> None:
        self._msgs[msg.dtm.isoformat()] = msg

    def get(self, dtm_str: str) -> Message | None:
        return self._msgs.get(dtm_str)


class ArchCurrent(MessageIndex):
    """The 'Current': SQLite Index + RAM Dict."""

    def __init__(self) -> None:
        # We subclass to override the worker and keep it sync for benchmarking
        super().__init__(maintain=False, db_path=":memory:")
        # FIX: Do not call _setup_db_schema() manually; MessageIndex/StorageWorker does it.

    def add(self, msg: Message) -> Message | None:
        # Mimic strict existing behavior: Insert Index -> Update RAM
        return super().add(msg)

    def get(self, msg: Message | None = None, **kwargs: Any) -> tuple[Message, ...]:
        # Queries DB for dtm, then fetches from RAM dict
        return super().get(msg=msg, **kwargs)


class ArchTier1_Orjson(MessageIndex):
    """Tier 1: 'Fat Database' using orjson in a TEXT column."""

    def __init__(self) -> None:
        super().__init__(maintain=False, db_path=":memory:")
        # Modified Schema: Drop separate index fields, add payload
        self._cu.execute("DROP TABLE messages")
        self._cu.execute("""
            CREATE TABLE messages (
                dtm TEXT PRIMARY KEY,
                hdr TEXT UNIQUE,
                payload_blob BLOB
            )
        """)
        self._cx.commit()

    def add(self, msg: Message) -> Message | None:
        # Serialize Payload
        payload_bytes = orjson.dumps(msg.payload)
        self._cu.execute(
            "INSERT OR REPLACE INTO messages (dtm, hdr, payload_blob) VALUES (?, ?, ?)",
            (msg.dtm.isoformat(), msg._pkt._hdr, payload_bytes),
        )
        return None

    def get(self, msg: Message) -> dict[str, Any]:  # type: ignore[override]
        # Fetch Payload and Deserialize
        self._cu.execute(
            "SELECT payload_blob FROM messages WHERE dtm = ?", (msg.dtm.isoformat(),)
        )
        row = self._cu.fetchone()
        if row:
            return orjson.loads(row[0])  # type: ignore[no-any-return]
        return {}


class ArchTier2_JsonB(MessageIndex):
    """Tier 2: 'Fat Database' using SQLite JSONB (Binary JSON)."""

    def __init__(self) -> None:
        if sqlite3.sqlite_version < "3.45.0":
            pytest.skip("SQLite < 3.45.0, JSONB not supported")

        super().__init__(maintain=False, db_path=":memory:")
        self._cu.execute("DROP TABLE messages")
        # Store as BLOB (JSONB is binary)
        self._cu.execute("""
            CREATE TABLE messages (
                dtm TEXT PRIMARY KEY,
                hdr TEXT UNIQUE,
                payload_jsonb BLOB
            )
        """)
        self._cx.commit()

    def add(self, msg: Message) -> Message | None:
        # JSONB requires passing raw text to the jsonb() function
        # We assume orjson returns bytes, so we might need decode() for standard SQL
        payload_str = orjson.dumps(msg.payload).decode()
        self._cu.execute(
            "INSERT OR REPLACE INTO messages (dtm, hdr, payload_jsonb) VALUES (?, ?, jsonb(?))",
            (msg.dtm.isoformat(), msg._pkt._hdr, payload_str),
        )
        return None

    def get(self, msg: Message) -> dict[str, Any]:  # type: ignore[override]
        # Fetch JSONB and parse
        self._cu.execute(
            "SELECT json(payload_jsonb) FROM messages WHERE dtm = ?",
            (msg.dtm.isoformat(),),
        )
        row = self._cu.fetchone()
        if row:
            return orjson.loads(row[0])  # type: ignore[no-any-return]
        return {}


# --- FIXTURES ---------------------------------------------------------------


@pytest.fixture(scope="module")
def dataset() -> list[Message]:
    return create_dummy_messages(1000)


@pytest.fixture(scope="function")
def arch_baseline() -> ArchBaselineDict:
    return ArchBaselineDict()


@pytest.fixture(scope="function")
def arch_current() -> ArchCurrent:
    return ArchCurrent()


@pytest.fixture(scope="function")
def arch_tier1() -> ArchTier1_Orjson:
    return ArchTier1_Orjson()


@pytest.fixture(scope="function")
def arch_tier2() -> ArchTier2_JsonB:
    return ArchTier2_JsonB()


# --- TESTS: WRITES ----------------------------------------------------------


def test_write_baseline(
    benchmark: Any, arch_baseline: ArchBaselineDict, dataset: list[Message]
) -> None:
    def run() -> None:
        for msg in dataset:
            arch_baseline.add(msg)

    benchmark(run)


def test_write_current(
    benchmark: Any, arch_current: ArchCurrent, dataset: list[Message]
) -> None:
    def run() -> None:
        for msg in dataset:
            arch_current.add(msg)

    benchmark(run)


def test_write_tier1_orjson(
    benchmark: Any, arch_tier1: ArchTier1_Orjson, dataset: list[Message]
) -> None:
    def run() -> None:
        for msg in dataset:
            arch_tier1.add(msg)

    benchmark(run)


def test_write_tier2_jsonb(
    benchmark: Any, arch_tier2: ArchTier2_JsonB, dataset: list[Message]
) -> None:
    def run() -> None:
        for msg in dataset:
            arch_tier2.add(msg)

    benchmark(run)


# --- TESTS: READS -----------------------------------------------------------


def test_read_baseline(
    benchmark: Any, arch_baseline: ArchBaselineDict, dataset: list[Message]
) -> None:
    # Pre-populate
    for msg in dataset:
        arch_baseline.add(msg)

    def run() -> None:
        for msg in dataset:
            _ = arch_baseline.get(msg.dtm.isoformat())

    benchmark(run)


def test_read_current(
    benchmark: Any, arch_current: ArchCurrent, dataset: list[Message]
) -> None:
    for msg in dataset:
        arch_current.add(msg)

    def run() -> None:
        for msg in dataset:
            _ = arch_current.get(msg)

    benchmark(run)


def test_read_tier1_orjson(
    benchmark: Any, arch_tier1: ArchTier1_Orjson, dataset: list[Message]
) -> None:
    for msg in dataset:
        arch_tier1.add(msg)

    def run() -> None:
        for msg in dataset:
            _ = arch_tier1.get(msg)

    benchmark(run)


def test_read_tier2_jsonb(
    benchmark: Any, arch_tier2: ArchTier2_JsonB, dataset: list[Message]
) -> None:
    for msg in dataset:
        arch_tier2.add(msg)

    def run() -> None:
        for msg in dataset:
            _ = arch_tier2.get(msg)

    benchmark(run)
