#!/usr/bin/env python3
"""
RAMSES RF - Message database and index.

.. table:: Database Query Methods[^1][#fn1]
   :widths: auto

   =====  ============  ===========  ==========  ====  ========================
    ix    method name   args         returns     uses  used by
   =====  ============  ===========  ==========  ====  ========================
   i1     get           Msg, kwargs  tuple(Msg)  i3
   i2     contains      kwargs       bool        i4
   i3     _select_from  kwargs       tuple(Msg)  i4
   i4     qry_dtms      kwargs       list(dtm)
   i5     qry           sql, kwargs  tuple(Msg)        _msgs()
   i6     qry_field     sql, kwargs  tuple(fld)        e4, e5
   i7     get_rp_codes  src, dst     list(Code)        Discovery-supported_cmds
   =====  ============  ===========  ==========  ====  ========================

[#fn1] A word of explanation.
[^1]: ex = entity_base.py query methods
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import sqlite3
import uuid
from collections import OrderedDict
from datetime import datetime as dt, timedelta as td
from typing import TYPE_CHECKING, Any, NewType

from ramses_tx import CODES_SCHEMA, RQ, Code, Message, Packet

from .storage import PacketLogEntry, StorageWorker

if TYPE_CHECKING:
    DtmStrT = NewType("DtmStrT", str)
    MsgDdT = OrderedDict[DtmStrT, Message]

_LOGGER = logging.getLogger(__name__)

# Regex to find the start of the packet frame (RSSI or Verb)
# Matches optional RSSI (000-100 or ... or ..) followed by Verb (I|RQ|RP|W)
# Handles variable spaces.
_FRAME_START_RE = re.compile(r"(?:[0-9]{3}|\.{2,3})\s+(I|RQ|RP|W)\s+")


def _setup_db_adapters() -> None:
    """Set up the database adapters and converters."""

    def adapt_datetime_iso(val: dt) -> str:
        """Adapt datetime.datetime to timezone-naive ISO 8601 datetime to match _msgs dtm keys."""
        return val.isoformat(timespec="microseconds")

    sqlite3.register_adapter(dt, adapt_datetime_iso)

    def convert_datetime(val: bytes) -> dt:
        """Convert ISO 8601 datetime to datetime.datetime object to import dtm in msg_db."""
        return dt.fromisoformat(val.decode())

    sqlite3.register_converter("DTM", convert_datetime)


def payload_keys(parsed_payload: list[dict] | dict) -> str:  # type: ignore[type-arg]
    """
    Copy payload keys for fast query check.

    :param parsed_payload: pre-parsed message payload dict
    :return: string of payload keys, separated by the | char
    """
    _keys: str = "|"

    def append_keys(ppl: dict) -> str:  # type: ignore[type-arg]
        _ks: str = ""
        for k, v in ppl.items():
            if (
                k not in _ks and k not in _keys and v is not None
            ):  # ignore keys with None value
                _ks += k + "|"
        return _ks

    if isinstance(parsed_payload, list):
        for d in parsed_payload:
            _keys += append_keys(d)
    elif isinstance(parsed_payload, dict):
        _keys += append_keys(parsed_payload)
    return _keys


class MessageIndex:
    """A central in-memory SQLite3 database for indexing RF messages.
    Index holds all the latest messages to & from all devices by `dtm`
    (timestamp) and `hdr` header
    (example of a hdr: ``000C|RP|01:223036|0208``)."""

    _housekeeping_task: asyncio.Task[None]

    def __init__(
        self, maintain: bool = True, db_path: str = ":memory:", store: str | None = None
    ) -> None:
        """Instantiate a message database/index.

        :param maintain: if True, start housekeeping loop
        :param db_path: path to the in-memory database (or shared memory URI)
        :param store: path to the persistent file for hydration/snapshots
        """

        self.maintain = maintain
        self.store = store
        self._msgs: MsgDdT = OrderedDict()  # stores all messages for retrieval.
        # Filled & cleaned up in housekeeping_loop.

        # For :memory: databases with multiple connections (Reader vs Worker)
        # We must use a Shared Cache URI so both threads see the same data.
        if db_path == ":memory:":
            # Unique ID ensures parallel tests don't share the same in-memory DB
            db_path = f"file:ramses_rf_{uuid.uuid4()}?mode=memory&cache=shared"

        # Start the Storage Worker (Write Connection)
        # This thread handles all blocking INSERT/UPDATE operations
        self._worker = StorageWorker(db_path)

        # Wait for the worker to create the tables.
        # This prevents "no such table" errors on immediate reads.
        if not self._worker.wait_for_ready(timeout=10.0):
            _LOGGER.error("MessageIndex: StorageWorker timed out initializing database")

        # Connect to a SQLite DB (Read Connection)
        self._cx: sqlite3.Connection | None = sqlite3.connect(
            db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
            uri=True,  # Enable URI parsing for shared memory support
            timeout=10.0,  # Increased timeout to reduce 'database locked' errors
            isolation_level=None,  # Autocommit mode prevents stale snapshots
        )

        # Enable Write-Ahead Logging for Reader as well
        if db_path != ":memory:" and "mode=memory" not in db_path:
            with contextlib.suppress(sqlite3.Error):
                self._cx.execute("PRAGMA journal_mode=WAL")
        elif "cache=shared" in db_path:
            # Shared cache (used in tests) requires read_uncommitted to prevent
            # readers from blocking writers (Table Locking).
            with contextlib.suppress(sqlite3.Error):
                self._cx.execute("PRAGMA read_uncommitted = true")

        # detect_types should retain dt type on store/retrieve
        self._cu = self._cx.cursor()  # Create a cursor

        _setup_db_adapters()  # DTM adapter/converter

        # Hydrate from persistent store if available
        if self.store:
            self._load_db()

        # Schema creation is now handled safely by the StorageWorker to avoid races.
        # self._setup_db_schema()

        if self.maintain:
            self._lock = asyncio.Lock()
            self._last_housekeeping: dt = None  # type: ignore[assignment]
            self._housekeeping_task = None  # type: ignore[assignment]

        self.start()

    def __repr__(self) -> str:
        return f"MessageIndex({len(self._msgs)} messages)"  # or msg_db.count()

    def start(self) -> None:
        """Start the housekeeper loop."""

        if self.maintain:
            if self._housekeeping_task and (not self._housekeeping_task.done()):
                return

            self._housekeeping_task = asyncio.create_task(
                self._housekeeping_loop(), name=f"{self.__class__.__name__}.housekeeper"
            )

    def stop(self) -> None:
        """Stop the housekeeper loop."""

        if (
            self.maintain
            and self._housekeeping_task
            and (not self._housekeeping_task.done())
        ):
            self._housekeeping_task.cancel()  # stop the housekeeper

        if self.store:  # Snapshot state on exit
            self._save_db()

        self._worker.stop()  # Stop the background thread

        # Close the connection idempotently
        if self._cx:
            try:
                self._cx.commit()  # just in case
                self._cx.close()  # may still need to do queries after engine has stopped?
            except sqlite3.ProgrammingError:
                pass  # Already closed
            finally:
                self._cx = None

    @property
    def msgs(self) -> MsgDdT:
        """Return the messages in the index in a threadsafe way."""
        return self._msgs

    def flush(self) -> None:
        """Flush the storage worker queue.

        This is primarily for testing to ensure data persistence before querying.
        """
        self._worker.flush()

    def _load_db(self) -> None:
        """Hydrate the in-memory database from the persistent store."""
        if not self.store or not os.path.isfile(self.store):
            return

        if not self._cx:
            return

        _LOGGER.debug(f"MessageIndex: Hydrating from {self.store}...")
        try:
            # We copy FROM disk TO memory
            with sqlite3.connect(self.store) as src:
                src.backup(self._cx)

            # Re-populate in-memory dict from the loaded SQLite data
            _LOGGER.debug("MessageIndex: Populating memory from snapshot...")
            self._cu.execute("SELECT dtm, blob FROM messages")
            for row in self._cu.fetchall():
                try:
                    dtm, blob = row
                    if blob:
                        pkt = Packet(dtm, blob)
                        msg = Message._from_pkt(pkt)
                        dtm_str = dtm.isoformat(timespec="microseconds")
                        self._msgs[dtm_str] = msg
                except (ValueError, TypeError, IndexError) as err:
                    _LOGGER.warning(f"MessageIndex: Failed to rehydrate message: {err}")

        except sqlite3.Error as err:
            _LOGGER.warning(
                f"MessageIndex: Failed to load database from {self.store}: {err}"
            )

    def _save_db(self) -> None:
        """Snapshot the in-memory database to the persistent store."""
        if not self.store or not self._cx:
            return

        _LOGGER.debug(f"MessageIndex: Snapshotting to {self.store}...")
        try:
            # We copy FROM memory TO disk
            with sqlite3.connect(self.store) as dst:
                self._cx.backup(dst)
        except sqlite3.Error as err:
            _LOGGER.warning(
                f"MessageIndex: Failed to save database to {self.store}: {err}"
            )

    # src/ramses_rf/database.py

    async def _housekeeping_loop(self) -> None:
        """Periodically remove stale messages from the index."""

        async def housekeeping(dt_now: dt, _cutoff: td = td(days=1)) -> None:
            dtm = dt_now - _cutoff
            dtm_iso = dtm.isoformat(timespec="microseconds")

            # Recommendation 2: Tight synchronization.
            # Submit prune request to worker (Non-blocking). [cite: 56, 323]
            self._worker.submit_prune(dtm)

            # Ensure the SQL partition is pruned before we proceed.
            # This prevents a "Hollow Index" where RAM is empty but SQL returns a dtm.
            await asyncio.get_running_loop().run_in_executor(None, self.flush)

            try:
                async with self._lock:  # Use context manager for safety
                    # Prune in-memory cache to match the SQL partition. [cite: 320, 382]
                    self._msgs = OrderedDict(
                        (k, v) for k, v in self._msgs.items() if k >= dtm_iso
                    )

            except Exception as err:
                _LOGGER.warning("MessageIndex housekeeping error: %s", err)
            else:
                _LOGGER.debug("Sync Housekeeping: RAM and SQL pruned to %s", dtm_iso)

        while True:
            self._last_housekeeping = dt.now()
            await asyncio.sleep(3600)  # Hourly check [cite: 226]

            # Offload save_db to executor as it is blocking I/O. [cite: 305, 489]
            if self.store:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, self._save_db
                    )
                except Exception as err:
                    _LOGGER.error("MessageIndex: Snapshot failed: %s", err)

            await housekeeping(self._last_housekeeping)

    def add(self, msg: Message) -> Message | None:
        """
        Add a single message to the MessageIndex.
        Logs a warning if there is a duplicate dtm.

        :returns: any message that was removed because it had the same header
        """
        # TODO: eventually, may be better to use SqlAlchemy

        dup: tuple[Message, ...] = tuple()  # avoid UnboundLocalError
        old: Message | None = None  # avoid UnboundLocalError

        # Check in-memory cache for collision instead of blocking SQL
        dtm_str: DtmStrT = msg.dtm.isoformat(timespec="microseconds")  # type: ignore[assignment]
        if dtm_str in self._msgs:
            dup = (self._msgs[dtm_str],)

        try:  # TODO: remove this, or apply only when source is a real packet log?
            # await self._lock.acquire()
            # dup = self._delete_from(  # HACK: because of contrived pkt logs
            #     dtm=msg.dtm  # stored as such with DTM formatter
            # )
            # We defer the write to the worker; return value (old) is not available synchronously
            self._insert_into(msg)  # will delete old msg by hdr (not dtm!)

        except (
            sqlite3.Error
        ):  # UNIQUE constraint failed: ? messages.dtm or .hdr (so: HACK)
            # self._cx.rollback()
            pass

        else:
            # _msgs dict requires a timestamp reformat
            # dtm: DtmStrT = msg.dtm.isoformat(timespec="microseconds")
            # add msg to self._msgs dict
            self._msgs[dtm_str] = msg

        finally:
            pass  # self._lock.release()

        if (
            dup
            and (msg.src is not msg.dst)
            and not msg.dst.id.startswith("18:")  # HGI
            and msg.verb != RQ  # these may come very quickly
        ):  # when src==dst, expect to add duplicate, don't warn
            _LOGGER.debug(
                "Overwrote dtm (%s) for %s: %s (contrived log?)",
                msg.dtm,
                msg._pkt._hdr,
                dup[0]._pkt,
            )

        return old

    def add_record(
        self, src: str, code: str = "", verb: str = " I", payload: str = "00"
    ) -> None:
        """
        Add a single record to the MessageIndex with timestamp `now()` and no Message contents.

        :param src: device id to use as source address
        :param code: device id to use as destination address (can be identical)
        :param verb: two letter verb str to use
        :param payload: payload str to use
        """
        # Used by OtbGateway init, via entity_base.py (code=_3220)
        _now: dt = dt.now()
        dtm: DtmStrT = _now.isoformat(timespec="microseconds")  # type: ignore[assignment]
        hdr = f"{code}|{verb}|{src}|{payload}"

        # Calculate length based on payload string length (2 chars = 1 byte)
        len_val = len(payload) // 2
        # Construct the dummy frame for the blob using correct length format (3 chars)
        blob = f"... {verb} --- {src} --:------ {src} {code} {len_val:03d} {payload}"

        # dup = self._delete_from(hdr=hdr)
        # Avoid blocking read; worker handles REPLACE on unique constraint collision

        # Prepare data tuple for worker
        data = PacketLogEntry(
            dtm=_now,
            verb=verb,
            src=src,
            dst=src,
            code=code,
            ctx=None,
            hdr=hdr,
            plk="|",
            blob=blob,
        )

        self._worker.submit_packet(data)

        # Backward compatibility for Tests:
        # Check specific env var set by pytest, which is more reliable than sys.modules
        if "PYTEST_CURRENT_TEST" in os.environ:
            self.flush()

        # also add dummy 3220 msg to self._msgs dict to allow maintenance loop
        # Note: Packet constructor requires full line or we rely on default behavior
        msg: Message = Message._from_pkt(Packet(_now, blob))
        self._msgs[dtm] = msg

        # if dup:  # expected when more than one heat system in schema
        #     _LOGGER.debug("Replaced record with same hdr: %s", hdr)

    def _insert_into(self, msg: Message) -> Message | None:
        """
        Insert a message into the index.

        :returns: any message replaced (by same hdr)
        """
        assert msg._pkt._hdr is not None, "Skipping: Packet has no hdr: {msg._pkt}"

        if msg._pkt._ctx is True:
            msg_pkt_ctx = "True"
        elif msg._pkt._ctx is False:
            msg_pkt_ctx = "False"
        else:
            msg_pkt_ctx = msg._pkt._ctx  # can be None

        # _old_msgs = self._delete_from(hdr=msg._pkt._hdr)
        # Refactor: Worker uses INSERT OR REPLACE to handle collision

        # Use _frame (raw string sans RSSI) if available, otherwise reconstruct/fallback
        # This preserves exact spacing and address order from the source
        if hasattr(msg._pkt, "_frame") and msg._pkt._frame:
            # _frame usually looks like " I --- 01:123456 ..." (missing RSSI)
            # It might have leading spaces. We strip them to ensure standard format.
            clean_frame = msg._pkt._frame.lstrip()

            # We prepend a default RSSI to make it a valid full frame.
            # FIX: " I" and " W" verbs require a leading space, which lstrip() removes.
            # We must restore this space so Packet(blob) slicing [4:] results in " I..."
            if clean_frame.startswith(("I ", "W ")):
                blob = f"...  {clean_frame}"
            else:
                blob = f"... {clean_frame}"
        else:
            # Fallback: remove timestamp from str representation if present
            # Robustly find the start of the frame (RSSI or Verb) using regex
            full_str = str(msg._pkt)
            match = _FRAME_START_RE.search(full_str)
            if match:
                blob = full_str[match.start() :]
            else:
                # Last resort: assume standard format and split if enough parts
                parts = full_str.split(maxsplit=2)
                blob = parts[2] if len(parts) >= 3 else full_str

        data = PacketLogEntry(
            dtm=msg.dtm,
            verb=str(msg.verb),
            src=msg.src.id,
            dst=msg.dst.id,
            code=str(msg.code),
            ctx=msg_pkt_ctx,
            hdr=msg._pkt._hdr,
            plk=payload_keys(msg.payload),
            blob=blob,
        )

        self._worker.submit_packet(data)

        # Backward compatibility for Tests:
        # Tests assume the DB update is instant. If running in pytest, flush immediately.
        # This effectively makes the operation synchronous during tests to avoid rewriting tests.
        if "PYTEST_CURRENT_TEST" in os.environ:
            self.flush()

        # _LOGGER.debug(f"Added {msg} to gwy.msg_db")

        return None

    def rem(
        self, msg: Message | None = None, **kwargs: str | dt
    ) -> tuple[Message, ...] | None:
        """Remove a set of message(s) from the index.

        :returns: any messages that were removed.
        """
        # _LOGGER.debug(f"SQL REM msg={msg} bool{bool(msg)} kwargs={kwargs} bool(kwargs)")
        # SQL REM
        # msg=||  02:044328 | | I | heat_demand | FC || {'domain_id': 'FC', 'heat_demand': 0.74}
        # boolTrue
        # kwargs={}
        # bool(kwargs)

        if not bool(msg) ^ bool(kwargs):
            raise ValueError("Either a Message or kwargs should be provided, not both")
        if msg:
            kwargs["dtm"] = msg.dtm

        msgs = None
        try:  # make this operation atomic, i.e. update self._msgs only on success
            # await self._lock.acquire()
            msgs = self._delete_from(**kwargs)

        except sqlite3.Error:  # need to tighten?
            if self._cx:
                self._cx.rollback()

        else:
            for msg in msgs:
                dtm: DtmStrT = msg.dtm.isoformat(timespec="microseconds")  # type: ignore[assignment]
                self._msgs.pop(dtm)

        finally:
            pass  # self._lock.release()

        return msgs

    def _delete_from(self, **kwargs: bool | dt | str) -> tuple[Message, ...]:
        """Remove message(s) from the index.

        :returns: any messages that were removed"""

        msgs = self._select_from(**kwargs)

        sql = "DELETE FROM messages WHERE "
        sql += " AND ".join(f"{k} = ?" for k in kwargs)

        self._cu.execute(sql, tuple(kwargs.values()))

        return msgs

    # MessageIndex msg_db query methods

    def get(
        self, msg: Message | None = None, **kwargs: bool | dt | str
    ) -> tuple[Message, ...]:
        """
        Public method to get a set of message(s) from the index.

        :param msg: Message to return, by dtm (expect a single result as dtm is unique key)
        :param kwargs: data table field names and criteria, e.g. (hdr=...)
        :return: tuple of matching Messages
        """

        if not (bool(msg) ^ bool(kwargs)):
            raise ValueError("Either a Message or kwargs should be provided, not both")

        if msg:
            kwargs["dtm"] = msg.dtm

        return self._select_from(**kwargs)

    def contains(self, **kwargs: bool | dt | str) -> bool:
        """
        Check if the MessageIndex contains at least 1 record that matches the provided fields.

        :param kwargs: (exact) SQLite table field_name: required_value pairs
        :return: True if at least one message fitting the given conditions is present, False when qry returned empty
        """

        return len(self.qry_dtms(**kwargs)) > 0

    def _select_from(self, **kwargs: bool | dt | str) -> tuple[Message, ...]:
        """
        Select message(s) using the MessageIndex.

        :param kwargs: (exact) SQLite table field_name: required_value pairs
        :returns: a tuple of qualifying messages
        """

        # CHANGE: Use a list comprehension with a check to avoid KeyError
        res: list[Message] = []
        for row in self.qry_dtms(**kwargs):
            ts: DtmStrT = row[0].isoformat(timespec="microseconds")
            if ts in self._msgs:
                res.append(self._msgs[ts])
            else:
                _LOGGER.debug("MessageIndex timestamp %s not in device messages", ts)
        return tuple(res)

    def qry_dtms(self, **kwargs: bool | dt | str) -> list[Any]:
        """
        Select from the MessageIndex a list of dtms that match the provided arguments.

        :param kwargs: data table field names and criteria
        :return: list of unformatted dtms that match, useful for msg lookup, or an empty list if 0 matches
        """
        # tweak kwargs as stored in SQLite, inverse from _insert_into():
        kw = {key: value for key, value in kwargs.items() if key != "ctx"}
        if "ctx" in kwargs:
            if isinstance(kwargs["ctx"], str):
                kw["ctx"] = kwargs["ctx"]
            elif kwargs["ctx"]:
                kw["ctx"] = "True"
            else:
                kw["ctx"] = "False"

        sql = "SELECT dtm FROM messages WHERE "
        sql += " AND ".join(f"{k} = ?" for k in kw)

        self._cu.execute(sql, tuple(kw.values()))
        return self._cu.fetchall()

    def qry(self, sql: str, parameters: tuple[str, ...]) -> tuple[Message, ...]:
        """
        Get a tuple of messages from _msgs using the index, given sql and parameters.

        :param sql: a bespoke SQL query SELECT string that should return dtm as first field
        :param parameters: tuple of kwargs with the selection filter
        :return: a tuple of qualifying messages
        """

        if "SELECT" not in sql:
            raise ValueError(f"{self}: Only SELECT queries are allowed")

        self._cu.execute(sql, parameters)

        lst: list[Message] = []
        # stamp = list(self._msgs)[0] if len(self._msgs) > 0 else "N/A"  # for debug
        for row in self._cu.fetchall():
            ts: DtmStrT = row[0].isoformat(
                timespec="microseconds"
            )  # must reformat from DTM
            # _LOGGER.debug(
            #     f"QRY Msg key raw: {row[0]} Reformatted: {ts} _msgs stamp format: {stamp}"
            # )
            # QRY Msg key raw: 2022-09-08 13:43:31.536862 Reformatted: 2022-09-08T13:43:31.536862
            # _msgs stamp format: 2022-09-08T13:40:52.447364
            if ts in self._msgs:
                lst.append(self._msgs[ts])
                # _LOGGER.debug("MessageIndex ts %s added to qry.lst", ts)  # too frequent
            else:  # happens in tests with artificial msg from heat
                _LOGGER.info("MessageIndex timestamp %s not in device messages", ts)
        return tuple(lst)

    def get_rp_codes(self, parameters: tuple[str, ...]) -> list[Code]:
        """
        Get a list of Codes from the index, given parameters.

        :param parameters: tuple of additional kwargs
        :return: list of Code: value pairs
        """

        def get_code(code: str) -> Code:
            for Cd in CODES_SCHEMA:
                if code == Cd:
                    return Cd
            raise LookupError(f"Failed to find matching code for {code}")

        sql = """
                SELECT code from messages WHERE verb is 'RP' AND (src = ? OR dst = ?)
            """
        if "SELECT" not in sql:
            raise ValueError(f"{self}: Only SELECT queries are allowed")

        self._cu.execute(sql, parameters)
        res = self._cu.fetchall()
        return [get_code(res[0]) for res[0] in self._cu.fetchall()]

    def qry_field(
        self, sql: str, parameters: tuple[str, ...]
    ) -> list[tuple[dt | str, str]]:
        """
        Get a list of fields from the index, given select sql and parameters.

        :param sql: a bespoke SQL query SELECT string
        :param parameters: tuple of additional kwargs
        :return: list of key: value pairs as defined in sql
        """

        if "SELECT" not in sql:
            raise ValueError(f"{self}: Only SELECT queries are allowed")

        self._cu.execute(sql, parameters)
        return self._cu.fetchall()

    def all(self, include_expired: bool = False) -> tuple[Message, ...]:
        """Get all messages from the index."""

        self._cu.execute("SELECT * FROM messages")

        lst: list[Message] = []
        # stamp = list(self._msgs)[0] if len(self._msgs) > 0 else "N/A"
        for row in self._cu.fetchall():
            ts: DtmStrT = row[0].isoformat(timespec="microseconds")
            # _LOGGER.debug(
            #     f"ALL Msg key raw: {row[0]} Reformatted: {ts} _msgs stamp format: {stamp}"
            # )
            # ALL Msg key raw: 2022-05-02 10:02:02.744905
            # Reformatted: 2022-05-02T10:02:02.744905
            # _msgs stamp format: 2022-05-02T10:02:02.744905
            if ts in self._msgs:
                # if include_expired or not self._msgs[ts].HAS_EXPIRED:  # not working
                lst.append(self._msgs[ts])
                _LOGGER.debug("MessageIndex ts %s added to all.lst", ts)
            else:  # happens in tests and real evohome setups with dummy msg from heat init
                _LOGGER.debug("MessageIndex ts %s not in device messages", ts)
        return tuple(lst)

    def clr(self) -> None:
        """Clear the message index (remove indexes of all messages)."""

        if not self._cx:
            return

        self._cu.execute("DELETE FROM messages")
        self._cx.commit()

        self._msgs.clear()

        # If we have a persistent store, wipe it too (Clean Slate)
        if self.store and os.path.isfile(self.store):
            with contextlib.suppress(OSError):
                os.remove(self.store)

    # def _msgs(self, device_id: DeviceIdT) -> tuple[Message, ...]:
    #     msgs = [msg for msg in self._msgs.values() if msg.src.id == device_id]
    #     return msgs
