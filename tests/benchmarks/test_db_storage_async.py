"""
Benchmark: Async Storage Architectures for ramses_rf
Mimics Home Assistant usage: SQLite in-memory, accessed via Executor threads.

Usage: pytest tests/benchmarks/test_db_storage_async.py --benchmark-columns=min,mean,max,ops
"""

import asyncio
import sqlite3
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime as dt, timedelta as td
from typing import Any, Generator

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

    for i in range(count):
        ts = base_time + td(milliseconds=i)
        # FIX: Ensure 'i' wraps around 255 so hex length stays exactly 2 chars
        # FIX: 'I' and 'W' verbs require a leading space in the frame structure
        safe_i = i % 256
        pkt_str = (
            f"000  I --- 01:123456 --:------ 01:123456 000A 006 0012345678{safe_i:02X}"
        )
        pkt = Packet.from_port(ts, pkt_str)
        msgs.append(Message(pkt))
    return msgs


# --- ARCHITECTURES ----------------------------------------------------------


class ArchBaselineDict:
    """The 'Original': Pure in-memory dictionary (Sync)."""

    def __init__(self) -> None:
        self._msgs: OrderedDict[str, Message] = OrderedDict()

    def add(self, msg: Message) -> None:
        self._msgs[msg.dtm.isoformat()] = msg

    def get(self, dtm_str: str) -> Message | None:
        return self._msgs.get(dtm_str)


class ArchCurrent(MessageIndex):
    """The 'Current': SQLite Index + RAM Dict."""

    def __init__(self) -> None:
        # MessageIndex handles schema and thread safety automatically
        super().__init__(maintain=False, db_path=":memory:")

    def add(self, msg: Message) -> Message | None:
        return super().add(msg)

    def get(self, msg: Message | None = None, **kwargs: Any) -> tuple[Message, ...]:
        return super().get(msg=msg, **kwargs)


class ArchTier1_Orjson(MessageIndex):
    """Tier 1: 'Fat Database' using orjson in a TEXT column."""

    def __init__(self) -> None:
        super().__init__(maintain=False, db_path=":memory:")
        # We assume the connection is set up by super(), we just modify the table
        # Note: In a threaded test, we must ensure this schema change happens
        # before the reads start.
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
        payload_bytes = orjson.dumps(msg.payload)
        self._cu.execute(
            "INSERT OR REPLACE INTO messages (dtm, hdr, payload_blob) VALUES (?, ?, ?)",
            (msg.dtm.isoformat(), msg._pkt._hdr, payload_bytes),
        )
        return None

    def get(self, msg: Message) -> dict[str, Any]:  # type: ignore[override]
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
        self._cu.execute("""
            CREATE TABLE messages (
                dtm TEXT PRIMARY KEY,
                hdr TEXT UNIQUE,
                payload_jsonb BLOB
            )
        """)
        self._cx.commit()

    def add(self, msg: Message) -> Message | None:
        payload_str = orjson.dumps(msg.payload).decode()
        self._cu.execute(
            "INSERT OR REPLACE INTO messages (dtm, hdr, payload_jsonb) VALUES (?, ?, jsonb(?))",
            (msg.dtm.isoformat(), msg._pkt._hdr, payload_str),
        )
        return None

    def get(self, msg: Message) -> dict[str, Any]:  # type: ignore[override]
        self._cu.execute(
            "SELECT json(payload_jsonb) FROM messages WHERE dtm = ?",
            (msg.dtm.isoformat(),),
        )
        row = self._cu.fetchone()
        if row:
            return orjson.loads(row[0])  # type: ignore[no-any-return]
        return {}


# --- FIXTURES ---------------------------------------------------------------


# FIX: Explicitly define event_loop to satisfy newer pytest-asyncio requirements
@pytest.fixture(scope="function")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


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


# --- HELPER: ASYNC BENCHMARK RUNNER -----------------------------------------


def run_async_benchmark(
    benchmark: Any, loop: asyncio.AbstractEventLoop, func: Any, *args: Any
) -> None:
    """
    Runs an async function in a synchronous benchmark wrapper.
    This measures the full Loop -> Executor -> DB -> Loop roundtrip latency.
    """
    # Create a dedicated executor for this benchmark run to mimic HA's executor
    executor = ThreadPoolExecutor(max_workers=2)

    async def _async_work() -> None:
        # This simulates: await hass.async_add_executor_job(func, *args)
        await loop.run_in_executor(executor, func, *args)

    def _sync_wrapper() -> None:
        # We use run_until_complete to drive the async task from the sync benchmark
        loop.run_until_complete(_async_work())

    try:
        benchmark(_sync_wrapper)
    finally:
        executor.shutdown(wait=True)


# --- TESTS: READS (ASYNC) ---------------------------------------------------
# We focus on Reads because Writes are handled by the StorageWorker in bg
# and don't block the loop in the same way.


def test_async_read_baseline(
    benchmark: Any,
    event_loop: asyncio.AbstractEventLoop,
    arch_baseline: ArchBaselineDict,
    dataset: list[Message],
) -> None:
    # Pre-populate
    for msg in dataset:
        arch_baseline.add(msg)

    # Baseline (Dict) is usually CPU bound, but we test offloading it anyway
    # to see the pure overhead of the Executor Context Switch.
    target_msg = dataset[0]

    run_async_benchmark(
        benchmark, event_loop, arch_baseline.get, target_msg.dtm.isoformat()
    )


def test_async_read_current(
    benchmark: Any,
    event_loop: asyncio.AbstractEventLoop,
    arch_current: ArchCurrent,
    dataset: list[Message],
) -> None:
    for msg in dataset:
        arch_current.add(msg)

    # In HA, accessing the DB (Index lookup) should be done in executor
    target_msg = dataset[0]

    run_async_benchmark(benchmark, event_loop, arch_current.get, target_msg)


def test_async_read_tier1_orjson(
    benchmark: Any,
    event_loop: asyncio.AbstractEventLoop,
    arch_tier1: ArchTier1_Orjson,
    dataset: list[Message],
) -> None:
    for msg in dataset:
        arch_tier1.add(msg)

    target_msg = dataset[0]

    run_async_benchmark(benchmark, event_loop, arch_tier1.get, target_msg)


def test_async_read_tier2_jsonb(
    benchmark: Any,
    event_loop: asyncio.AbstractEventLoop,
    arch_tier2: ArchTier2_JsonB,
    dataset: list[Message],
) -> None:
    for msg in dataset:
        arch_tier2.add(msg)

    target_msg = dataset[0]

    run_async_benchmark(benchmark, event_loop, arch_tier2.get, target_msg)
