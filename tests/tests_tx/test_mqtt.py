"""Tests for the MQTT transport implementation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from paho.mqtt import client as mqtt

from ramses_tx import exceptions as exc
from ramses_tx.const import SZ_RAMSES_GATEWAY
from ramses_tx.transports.mqtt import MqttTransport, _TokenBucket, validate_topic_path

# --- Fixtures ---


@pytest.fixture
def mock_protocol() -> MagicMock:
    """Return a mock protocol."""
    protocol = MagicMock()
    protocol.pause_writing = MagicMock()
    protocol.resume_writing = MagicMock()
    return protocol


@pytest.fixture
def mock_mqtt_client() -> Generator[MagicMock, None, None]:
    """Return a mocked Paho MQTT client."""
    with patch("ramses_tx.transports.mqtt.mqtt.Client") as mock_cls:
        client = mock_cls.return_value
        client.connect_async = MagicMock()
        client.loop_start = MagicMock()
        client.loop_stop = MagicMock()
        client.subscribe = MagicMock()
        client.unsubscribe = MagicMock()
        client.publish = MagicMock()
        client.disconnect = MagicMock()
        yield client


@pytest.fixture
async def transport(
    mock_protocol: MagicMock, mock_mqtt_client: MagicMock
) -> MqttTransport:
    """Return an initialized MqttTransport."""
    url = f"mqtt://user:pass@localhost:1883/{SZ_RAMSES_GATEWAY}/01:123456"
    # FIX: Use the running loop provided by the async fixture context
    loop = asyncio.get_running_loop()
    return MqttTransport(url, mock_protocol, loop=loop)


# --- Tests: validate_topic_path ---


def test_validate_topic_path_valid() -> None:
    """Test valid topic paths."""
    base = SZ_RAMSES_GATEWAY
    assert validate_topic_path(f"{base}/01:123456") == f"{base}/01:123456"
    assert validate_topic_path(f"/{base}/01:123456") == f"{base}/01:123456"


def test_validate_topic_path_default() -> None:
    """Test default topic path generation."""
    assert validate_topic_path("") == f"{SZ_RAMSES_GATEWAY}/+"
    assert validate_topic_path(SZ_RAMSES_GATEWAY) == f"{SZ_RAMSES_GATEWAY}/+"


def test_validate_topic_path_invalid() -> None:
    """Test invalid topic paths raise ValueError."""
    with pytest.raises(ValueError, match="Invalid topic path"):
        validate_topic_path("invalid_prefix/123")
    with pytest.raises(ValueError, match="Invalid topic path"):
        validate_topic_path(f"{SZ_RAMSES_GATEWAY}/too/many/parts")


# --- Tests: _TokenBucket ---


@pytest.mark.asyncio
async def test_token_bucket_consume_success() -> None:
    """Test successful token consumption."""
    bucket = _TokenBucket(max_tokens=10, time_window=10)
    # Initially full
    assert await bucket.consume() is True


@pytest.mark.asyncio
async def test_token_bucket_refill_logic() -> None:
    """Test that tokens refill over time."""
    bucket = _TokenBucket(max_tokens=10, time_window=10)
    bucket._tokens = 0.0  # Empty bucket

    with patch("ramses_tx.transports.mqtt.perf_counter") as mock_time:
        mock_time.return_value = 100.0
        bucket._timestamp = 99.0  # 1 second elapsed
        # Rate is 1 token/sec (10/10) -> Refill 1.0 -> Consume 1.0 -> Result 0.0
        assert await bucket.consume() is True
        assert bucket._tokens == 0.0


@pytest.mark.asyncio
async def test_token_bucket_debt_drop() -> None:
    """Test that requests are dropped if debt is too high."""
    bucket = _TokenBucket(max_tokens=10, time_window=10)
    bucket._tokens = -10.0

    with patch("ramses_tx.transports.mqtt.perf_counter") as mock_time:
        mock_time.return_value = 100.0
        bucket._timestamp = 100.0
        assert await bucket.consume() is False


@pytest.mark.asyncio
async def test_token_bucket_throttle() -> None:
    """Test that the bucket sleeps (throttles) when in minor debt."""
    # Use params that allow debt without dropping (Rate > 1.0)
    # Rate = 2.0. Drop threshold = 1.0 - 2.0 = -1.0.
    bucket = _TokenBucket(max_tokens=20, time_window=10)
    bucket._tokens = -0.5  # Debt, but above threshold (-0.5 > -1.0)

    with (
        patch("ramses_tx.transports.mqtt.perf_counter") as mock_time,
        patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        mock_time.return_value = 100.0
        bucket._timestamp = 100.0

        assert await bucket.consume() is True
        mock_sleep.assert_called_once()


def test_token_bucket_repr() -> None:
    """Test string representation."""
    bucket = _TokenBucket(10, 10)
    assert "/" in repr(bucket)


# --- Tests: MqttTransport Lifecycle ---


def test_init(transport: MqttTransport) -> None:
    """Test initialization parses URL and sets up client."""
    assert transport._username == "user"
    assert transport._password == "pass"
    assert transport._topic_base == f"{SZ_RAMSES_GATEWAY}/01:123456"
    assert transport._mqtt_qos == 0


def test_attempt_connection_success(
    transport: MqttTransport, mock_mqtt_client: MagicMock
) -> None:
    """Test successful connection attempt."""
    # Reset connecting flag set by init
    transport._connecting = False
    transport._attempt_connection()
    mock_mqtt_client.connect_async.assert_called_with("localhost", 1883, 60)
    mock_mqtt_client.loop_start.assert_called()


def test_attempt_connection_fail(
    transport: MqttTransport, mock_mqtt_client: MagicMock
) -> None:
    """Test connection failure schedules reconnect."""
    # Reset connecting flag set by init so we can try again
    transport._connecting = False
    mock_mqtt_client.connect_async.side_effect = Exception("Boom")

    with patch.object(transport, "_schedule_reconnect") as mock_sched:
        transport._attempt_connection()
        mock_sched.assert_called_once()


def test_on_connect_success(
    transport: MqttTransport, mock_mqtt_client: MagicMock
) -> None:
    """Test _on_connect callback logic."""
    rc = MagicMock()
    rc.is_failure = False
    rc.getName.return_value = "Success"

    transport._topic_base = f"{SZ_RAMSES_GATEWAY}/+"
    transport._topic_sub = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"
    transport._data_wildcard_topic = f"{SZ_RAMSES_GATEWAY}/+/rx"

    transport._on_connect(transport.client, None, {}, rc, None)

    mock_mqtt_client.subscribe.assert_any_call(
        f"{SZ_RAMSES_GATEWAY}/01:123456/rx", qos=0
    )
    mock_mqtt_client.unsubscribe.assert_called_with(f"{SZ_RAMSES_GATEWAY}/+/rx")
    assert transport._data_wildcard_topic == ""


def test_on_connect_failure(transport: MqttTransport) -> None:
    """Test _on_connect handles failure RC."""
    rc = MagicMock()
    rc.is_failure = True
    with patch.object(transport, "_schedule_reconnect") as mock_sched:
        transport._on_connect(transport.client, None, {}, rc, None)
        mock_sched.assert_called_once()


def test_on_disconnect(transport: MqttTransport, mock_protocol: MagicMock) -> None:
    """Test _on_disconnect logic."""
    transport._connected = True
    transport._topic_sub = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"

    rc = MagicMock()
    rc.getName.return_value = "Disconnect"

    transport._on_disconnect(transport.client, None, rc)

    assert transport._connected is False
    mock_protocol.pause_writing.assert_called_once()


# --- Tests: Message Handling ---


def test_on_message_online_fresh_connection(
    transport: MqttTransport, mock_protocol: MagicMock
) -> None:
    """Test handling of 'online' payload for a fresh connection."""
    # Use a MagicMock to avoid Paho internal encoding issues
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456"
    msg.payload = b"online"

    # Ensure we start disconnected
    transport._connected = False

    transport._on_message(transport.client, None, msg)

    assert transport._connected is True
    assert transport._topic_pub == f"{SZ_RAMSES_GATEWAY}/01:123456/tx"
    # resume_writing is NOT called on first connection
    mock_protocol.resume_writing.assert_not_called()


def test_on_message_online_reconnection(
    transport: MqttTransport, mock_protocol: MagicMock
) -> None:
    """Test handling of 'online' payload when reconnecting (resuming)."""
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456"
    msg.payload = b"online"

    # Simulate already connected (re-establishing logic)
    transport._connected = True

    # FIX: Mock the event loop to intercept the schedule call
    mock_loop = MagicMock()
    transport._loop = mock_loop

    transport._on_message(transport.client, None, msg)

    # Verify that resume_writing was SCHEDULED on the loop
    mock_loop.call_soon_threadsafe.assert_called_once_with(mock_protocol.resume_writing)


def test_on_message_offline(transport: MqttTransport, mock_protocol: MagicMock) -> None:
    """Test handling of 'offline' payload."""
    transport._topic_sub = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456"
    msg.payload = b"offline"

    transport._on_message(transport.client, None, msg)
    mock_protocol.pause_writing.assert_called()


def test_on_message_infer_connection(transport: MqttTransport) -> None:
    """Test inferring connection from a data topic."""
    transport._connection_established = False
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"
    msg.payload = json.dumps({"ts": "2023-01-01T12:00:00", "msg": "I ..."}).encode()

    with patch.object(transport, "_frame_read"):
        transport._on_message(transport.client, None, msg)

    assert transport._connected is True
    assert transport._topic_pub == f"{SZ_RAMSES_GATEWAY}/01:123456/tx"


def test_on_message_invalid_json(transport: MqttTransport) -> None:
    """Test invalid JSON payload is ignored."""
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"
    msg.payload = b"junk"

    with patch("ramses_tx.transports.mqtt._LOGGER") as mock_logger:
        transport._on_message(transport.client, None, msg)
        mock_logger.warning.assert_called()


def test_on_message_old_timestamp(transport: MqttTransport) -> None:
    """Test warning for old timestamps."""
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"
    msg.payload = json.dumps({"ts": "2000-01-01T12:00:00", "msg": "I ..."}).encode()

    with (
        patch("ramses_tx.transports.mqtt._LOGGER") as mock_logger,
        patch.object(transport, "_frame_read"),
    ):
        transport._on_message(transport.client, None, msg)
        assert any("SNTP" in str(c) for c in mock_logger.warning.call_args_list)


def test_on_message_transport_error(transport: MqttTransport) -> None:
    """Test TransportError in _frame_read is caught or raised."""
    msg = MagicMock()
    msg.topic = f"{SZ_RAMSES_GATEWAY}/01:123456/rx"
    msg.payload = json.dumps({"ts": "2023-01-01T12:00:00", "msg": "I ..."}).encode()

    with (
        patch.object(transport, "_frame_read", side_effect=exc.TransportError),
        pytest.raises(exc.TransportError),
    ):
        transport._on_message(transport.client, None, msg)


# --- Tests: Write Frame ---


@pytest.mark.asyncio
async def test_write_frame_not_connected(
    transport: MqttTransport, mock_mqtt_client: MagicMock
) -> None:
    """Test write dropped if not connected."""
    transport._connected = False
    await transport.write_frame("I ...")
    mock_mqtt_client.publish.assert_not_called()


@pytest.mark.asyncio
async def test_write_frame_rate_limited(
    transport: MqttTransport, mock_mqtt_client: MagicMock
) -> None:
    """Test write dropped if rate limited."""
    transport._connected = True
    with patch.object(transport._rate_limiter, "consume", return_value=False):
        await transport.write_frame("I ...")
        mock_mqtt_client.publish.assert_not_called()


@pytest.mark.asyncio
async def test_write_frame_success(
    transport: MqttTransport, mock_mqtt_client: MagicMock
) -> None:
    """Test successful write delegates to publish."""
    transport._connected = True
    transport._topic_pub = "tx/topic"
    with patch.object(transport._rate_limiter, "consume", return_value=True):
        await transport.write_frame("I ...")

    mock_mqtt_client.publish.assert_called()


def test_publish_failure(transport: MqttTransport, mock_mqtt_client: MagicMock) -> None:
    """Test publish failure handling."""
    transport._connected = True
    transport._topic_pub = "tx/topic"

    # Simulate MQTT error code
    info = MagicMock()
    info.rc = mqtt.MQTT_ERR_NO_CONN
    mock_mqtt_client.publish.return_value = info

    with patch.object(transport, "_schedule_reconnect") as mock_sched:
        transport._publish("payload")
        assert transport._connected is False
        mock_sched.assert_called_once()


# --- Tests: Cleanup ---


def test_close(transport: MqttTransport, mock_mqtt_client: MagicMock) -> None:
    """Test close cleanup."""
    transport._connected = True
    mock_task = MagicMock()
    transport._reconnect_task = mock_task

    transport._close()

    assert transport._connected is False
    # Check the reference we saved, since transport._reconnect_task is now None
    mock_task.cancel.assert_called()
    mock_mqtt_client.disconnect.assert_called()
    mock_mqtt_client.loop_stop.assert_called()


# --- Tests: Reconnection Logic ---


@pytest.mark.asyncio
async def test_reconnect_after_delay(transport: MqttTransport) -> None:
    """Test the reconnect backoff loop."""
    transport._current_reconnect_interval = 0.1
    transport._reconnect_backoff = 2.0
    transport._max_reconnect_interval = 1.0

    with (
        patch("asyncio.sleep", new_callable=AsyncMock),
        patch.object(transport, "_attempt_connection") as mock_attempt,
    ):
        await transport._reconnect_after_delay()

        mock_attempt.assert_called_once()
        assert transport._current_reconnect_interval == 0.2
