from __future__ import annotations

from datetime import datetime as dt
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ramses_rf.entity_state import EntityState
from ramses_tx.const import I_, Code


class DummyMsg:
    """A lightweight mock message to accurately track property accesses."""

    def __init__(self, src_id: str, code: Code, payload_dict: dict[str, Any]) -> None:
        self.src = MagicMock()
        self.src.id = src_id
        self.dst = MagicMock()
        self.dst.id = "01:000000"
        self.verb = I_
        self.code = code
        self.dtm = dt.now()

        self._pkt = MagicMock()
        self._pkt._ctx = False
        self._expired = False

        self._payload_dict = payload_dict
        self.payload_access_count = 0

    @property
    def payload(self) -> dict[str, Any]:
        """Track how many times the payload is evaluated."""
        self.payload_access_count += 1
        return self._payload_dict


@pytest.fixture
def zone_entity() -> EntityState:
    """Fixture to provide a standard mocked Zone EntityState."""
    mock_dev = MagicMock()
    mock_dev.id = "04:123456_00"  # _00 makes it a Zone

    mock_gwy = MagicMock()
    mock_gwy.message_store = MagicMock()
    mock_gwy.message_store.log_by_dtm = []

    return EntityState(mock_dev, mock_gwy)


@pytest.mark.asyncio
async def test_step1_relevance_check_passes(zone_entity: EntityState) -> None:
    """Step 1: Does the message pass the initial L4 routing check?"""
    # Create a message from the correct parent device (04:123456)
    msg = DummyMsg("04:123456", Code._30C9, {"temperature": 21.0})

    # The L4 check just looks at the first 9 chars of the ID. It should pass.
    assert zone_entity._is_relevant_msg(msg) is True


@pytest.mark.asyncio
async def test_step2_cache_drops_invalid_zone_payload(
    zone_entity: EntityState,
) -> None:
    """Step 2: Why did our previous test return None?"""
    # Payload lacks 'zone_idx', so the Zone cache builder should reject it
    msg = DummyMsg("04:123456", Code._30C9, {"temperature": 21.0})
    zone_entity._gwy.message_store.log_by_dtm = [msg]

    cache = await zone_entity._build_state_cache()

    # Assert the cache is empty because the payload didn't match the zone
    assert len(cache.get_all()) == 0
    # The payload property is accessed exactly 3 times in the zone check logic
    assert msg.payload_access_count == 3


@pytest.mark.asyncio
async def test_step3_cache_keeps_valid_zone_payload(
    zone_entity: EntityState,
) -> None:
    """Step 3: What happens when the payload is correctly formatted?"""
    # We add "zone_idx": "00" to match our mock_dev.id
    msg = DummyMsg(
        "04:123456",
        Code._30C9,
        {"temperature": 21.0, "zone_idx": "00"},
    )
    zone_entity._gwy.message_store.log_by_dtm = [msg]

    cache = await zone_entity._build_state_cache()

    # The cache should now successfully store the message
    assert len(cache.get_all()) == 1
    assert msg.payload_access_count == 3


@pytest.mark.asyncio
async def test_step4_get_value_causes_full_store_iteration(
    zone_entity: EntityState,
) -> None:
    """Step 4: Proving the O(N^2) CPU bug with correct payload data."""
    packet_count = 5000

    # Create an artificially large global log
    mock_log = [
        DummyMsg(
            "04:123456",
            Code._30C9,
            {"temperature": 21.0, "zone_idx": "00"},
        )
        for _ in range(packet_count)
    ]
    zone_entity._gwy.message_store.log_by_dtm = mock_log

    with patch.object(
        zone_entity, "_is_relevant_msg", wraps=zone_entity._is_relevant_msg
    ) as spy_is_relevant:
        # Action: Query the state
        result = await zone_entity.get_value(Code._30C9)

        # Note: _msg_value_msg strips 'zone_idx' out before returning to HA
        assert result == {"temperature": 21.0}

        # Assert 1: The system evaluated EVERY packet in the global history
        assert spy_is_relevant.call_count == packet_count

        # Assert 2: The system evaluated the payload >=3x on EVERY relevant packet
        for msg in mock_log:
            assert msg.payload_access_count >= 3
