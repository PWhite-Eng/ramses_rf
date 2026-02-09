"""RAMSES RF - MQTT transport implementation."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime as dt, timedelta as td
from time import perf_counter
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import parse_qs, unquote, urlparse

from paho.mqtt import MQTTException, client as mqtt

try:
    from paho.mqtt.enums import CallbackAPIVersion
except ImportError:
    CallbackAPIVersion = None  # type: ignore[assignment,misc]

from .. import exceptions as exc
from ..const import (
    DUTY_CYCLE_DURATION,
    MAX_TRANSMIT_RATE_TOKENS,
    SZ_ACTIVE_HGI,
    SZ_IS_EVOFW3,
    SZ_RAMSES_GATEWAY,
)
from ..schemas import DeviceIdT
from .base import DBG_FORCE_FRAME_LOGGING, _FullTransport, _normalise

if TYPE_CHECKING:
    from ..protocol import RamsesProtocolT

_LOGGER = logging.getLogger(__name__)


def validate_topic_path(path: str) -> str:
    """Test the topic path and normalize it."""
    new_path = path or SZ_RAMSES_GATEWAY
    if new_path.startswith("/"):
        new_path = new_path[1:]
    if not new_path.startswith(SZ_RAMSES_GATEWAY):
        raise ValueError(f"Invalid topic path: {path}")
    if new_path == SZ_RAMSES_GATEWAY:
        new_path += "/+"
    if len(new_path.split("/")) != 3:
        raise ValueError(f"Invalid topic path: {path}")
    return new_path


class _TokenBucket:
    """A token bucket rate limiter implementation.

    Encapsulates the logic for maintaining token counts and calculating
    delays or drops based on a defined duty cycle.
    """

    def __init__(self, max_tokens: int, time_window: int) -> None:
        self._max_capacity: Final[float] = float(max_tokens)
        self._token_rate: Final[float] = self._max_capacity / time_window
        self._tokens: float = self._max_capacity * 2
        self._timestamp: float = perf_counter()

        # Dynamic capacity that can grow/shrink within bounds
        self._current_capacity: float = self._max_capacity * 2

    async def consume(self, disable_limits: bool = False) -> bool:
        """Attempt to consume a token.

        Returns:
            bool: True if the token was consumed (or limits disabled),
                  False if the request should be dropped.
        """
        timestamp = perf_counter()
        elapsed = timestamp - self._timestamp
        self._timestamp = timestamp

        # Refill tokens
        self._tokens = min(
            self._tokens + elapsed * self._token_rate, self._current_capacity
        )

        # Check if we are too far behind to even queue (Drop condition)
        # Using the original logic: if < 1.0 - rate, we are in debt
        if self._tokens < 1.0 - self._token_rate and not disable_limits:
            return False

        self._tokens -= 1.0

        # Adjust capacity logic (preserved from original implementation)
        if self._current_capacity > self._max_capacity:
            self._current_capacity = min(self._current_capacity, self._tokens)
            self._current_capacity = max(self._current_capacity, self._max_capacity)

        # Calculate sleep if in debt (Throttle condition)
        if self._tokens < 0.0 and not disable_limits:
            delay = (0 - self._tokens) / self._token_rate
            await asyncio.sleep(delay)

        return True

    def __repr__(self) -> str:
        return f"{self._tokens:.2f}/{self._current_capacity:.2f}"


class _MqttTransportAbstractor:
    """Do the bare minimum to abstract a transport from its underlying class."""

    def __init__(
        self,
        broker_url: str,
        protocol: RamsesProtocolT,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._broker_url = urlparse(broker_url)
        self._protocol = protocol
        self._loop = loop or asyncio.get_event_loop()


class MqttTransport(_FullTransport, _MqttTransportAbstractor):
    """Send/receive packets to/from ramses_esp via MQTT."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._username = unquote(self._broker_url.username or "")
        self._password = unquote(self._broker_url.password or "")
        self._topic_base = validate_topic_path(self._broker_url.path)
        self._topic_pub = ""
        self._topic_sub = ""
        self._data_wildcard_topic = ""
        self._mqtt_qos = int(parse_qs(self._broker_url.query).get("qos", ["0"])[0])

        self._connected = False
        self._connecting = False
        self._connection_established = False
        self._extra[SZ_IS_EVOFW3] = True

        self._reconnect_interval = 5.0
        self._max_reconnect_interval = 300.0
        self._reconnect_backoff = 1.5
        self._current_reconnect_interval = self._reconnect_interval
        self._reconnect_task: asyncio.Task[None] | None = None

        self._log_all = kwargs.pop("log_all", False)

        # Composition: Rate limiter logic extracted to helper class
        self._rate_limiter = _TokenBucket(
            max_tokens=MAX_TRANSMIT_RATE_TOKENS,
            time_window=DUTY_CYCLE_DURATION,
        )

        self.client = mqtt.Client(
            protocol=mqtt.MQTTv5, callback_api_version=CallbackAPIVersion.VERSION2
        )
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.username_pw_set(self._username, self._password)
        self._attempt_connection()

    def _attempt_connection(self) -> None:
        if self._connecting or self._connected:
            return
        self._connecting = True
        try:
            self.client.connect_async(
                str(self._broker_url.hostname or "localhost"),
                self._broker_url.port or 1883,
                60,
            )
            self.client.loop_start()
        except Exception as err:
            _LOGGER.error(f"Failed to initiate MQTT connection: {err}")
            self._connecting = False
            self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        if self._closing or self._reconnect_task:
            return
        _LOGGER.info(
            f"Scheduling MQTT reconnect in {self._current_reconnect_interval} seconds"
        )
        self._reconnect_task = self._loop.create_task(
            self._reconnect_after_delay(), name="MqttTransport._reconnect_after_delay()"
        )

    async def _reconnect_after_delay(self) -> None:
        try:
            await asyncio.sleep(self._current_reconnect_interval)
            self._current_reconnect_interval = min(
                self._current_reconnect_interval * self._reconnect_backoff,
                self._max_reconnect_interval,
            )
            _LOGGER.info("Attempting MQTT reconnection...")
            self._attempt_connection()
        except asyncio.CancelledError:
            pass
        finally:
            self._reconnect_task = None

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        reason_code: Any,
        properties: Any | None,
    ) -> None:
        self._connecting = False
        if reason_code.is_failure:
            _LOGGER.error(f"MQTT connection failed: {reason_code.getName()}")
            self._schedule_reconnect()
            return

        _LOGGER.info(f"MQTT connected: {reason_code.getName()}")
        self._current_reconnect_interval = self._reconnect_interval
        if self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None

        self.client.subscribe(self._topic_base)
        if self._topic_base.endswith("/+") and not (
            hasattr(self, "_topic_sub") and self._topic_sub
        ):
            data_wildcard = self._topic_base.replace("/+", "/+/rx")
            self.client.subscribe(data_wildcard, qos=self._mqtt_qos)
            self._data_wildcard_topic = data_wildcard
            _LOGGER.debug(f"Subscribed to data wildcard: {data_wildcard}")

        if hasattr(self, "_topic_sub") and self._topic_sub:
            self.client.subscribe(self._topic_sub, qos=self._mqtt_qos)
            if getattr(self, "_data_wildcard_topic", ""):
                try:
                    self.client.unsubscribe(self._data_wildcard_topic)
                finally:
                    self._data_wildcard_topic = ""

    def _on_connect_fail(self, client: mqtt.Client, userdata: Any) -> None:
        _LOGGER.error("MQTT connection failed")
        self._connecting = False
        self._connected = False
        if not self._closing:
            self._schedule_reconnect()

    def _on_disconnect(
        self, client: mqtt.Client, userdata: Any, *args: Any, **kwargs: Any
    ) -> None:
        reason_code = args[0] if len(args) >= 1 else None
        reason_name = (
            reason_code.getName()
            if reason_code is not None and hasattr(reason_code, "getName")
            else str(reason_code)
        )
        _LOGGER.warning(f"MQTT disconnected: {reason_name}")
        was_connected = self._connected
        self._connected = False

        if was_connected and hasattr(self, "_topic_sub") and self._topic_sub:
            device_topic = self._topic_sub[:-3]
            _LOGGER.warning(f"{self}: the MQTT device is offline: {device_topic}")
            if hasattr(self, "_protocol"):
                self._protocol.pause_writing()

        if not self._closing:
            self._schedule_reconnect()

    def _create_connection(self, msg: mqtt.MQTTMessage) -> None:
        if self._connected:
            _LOGGER.info("MQTT device came back online - resuming writing")
            self._loop.call_soon_threadsafe(self._protocol.resume_writing)
            return

        _LOGGER.info("MQTT device is online - establishing connection")
        self._connected = True
        self._extra[SZ_ACTIVE_HGI] = msg.topic[-9:]
        self._topic_pub = msg.topic + "/tx"
        self._topic_sub = msg.topic + "/rx"
        self.client.subscribe(self._topic_sub, qos=self._mqtt_qos)

        if getattr(self, "_data_wildcard_topic", ""):
            try:
                self.client.unsubscribe(self._data_wildcard_topic)
            finally:
                self._data_wildcard_topic = ""

        if not self._connection_established:
            self._connection_established = True
            # msg.topic is str, but _make_connection expects DeviceIdT
            self._make_connection(gwy_id=cast(DeviceIdT, msg.topic[-9:]))
        else:
            _LOGGER.info("MQTT reconnected - protocol connection already established")

    def _parse_payload(self, payload: bytes) -> tuple[str, str]:
        """Parse the raw MQTT payload into a timestamp and frame string.

        Raises:
            json.JSONDecodeError: If payload is not valid JSON.
            KeyError: If payload is missing required keys.
        """
        data = json.loads(payload)
        return data["ts"], data["msg"]

    def _on_message(
        self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ) -> None:
        if DBG_FORCE_FRAME_LOGGING:
            _LOGGER.warning("Rx: %s", msg.payload)
        elif self._log_all and _LOGGER.getEffectiveLevel() == logging.INFO:
            _LOGGER.info("mq Rx: %s", msg.payload)

        # Handle Online/Offline Status messages
        if msg.topic[-3:] != "/rx":
            if msg.payload == b"offline":
                is_sub_topic = (
                    hasattr(self, "_topic_sub")
                    and self._topic_sub
                    and msg.topic == self._topic_sub[:-3]
                )
                if is_sub_topic or not hasattr(self, "_topic_sub"):
                    _LOGGER.warning(
                        f"{self}: the ESP device is offline (via LWT): {msg.topic}"
                    )
                    if hasattr(self, "_protocol"):
                        self._protocol.pause_writing()
            elif msg.payload == b"online":
                _LOGGER.info(
                    f"{self}: the ESP device is online (via status): {msg.topic}"
                )
                self._create_connection(msg)
            return

        # Infer connection from data topic if not established
        if not self._connection_established and msg.topic.endswith("/rx"):
            topic_parts = msg.topic.split("/")
            if len(topic_parts) >= 3 and topic_parts[-2] not in ("+", "*"):
                gateway_id = topic_parts[-2]
                _LOGGER.info(
                    f"Inferring gateway connection from data topic: {gateway_id}"
                )
                self._topic_pub = f"{'/'.join(topic_parts[:-1])}/tx"
                self._topic_sub = msg.topic
                self._extra[SZ_ACTIVE_HGI] = gateway_id
                self._connected = True
                self._connection_established = True
                self._make_connection(gwy_id=cast(DeviceIdT, gateway_id))

                try:
                    self.client.subscribe(self._topic_sub, qos=self._mqtt_qos)
                except Exception as err:
                    _LOGGER.debug(f"Error subscribing specific topic: {err}")
                if getattr(self, "_data_wildcard_topic", ""):
                    try:
                        self.client.unsubscribe(self._data_wildcard_topic)
                    finally:
                        self._data_wildcard_topic = ""

        # Process the Frame
        try:
            ts_iso, frame_str = self._parse_payload(msg.payload)
        except (json.JSONDecodeError, KeyError):
            _LOGGER.warning("%s < Can't decode JSON (ignoring)", msg.payload)
            return

        dtm = dt.fromisoformat(ts_iso)
        if dtm.tzinfo is not None:
            dtm = dtm.astimezone().replace(tzinfo=None)
        if dtm < dt.now() - td(days=90):
            _LOGGER.warning(
                f"{self}: Have you configured the SNTP settings on the ESP?"
            )

        try:
            self._frame_read(dtm.isoformat(), _normalise(frame_str))
        except exc.TransportError:
            if not self._closing:
                raise

    async def write_frame(self, frame: str, disable_tx_limits: bool = False) -> None:
        if not self._connected:
            _LOGGER.debug(f"{self}: Dropping write - MQTT not connected")
            return

        # Delegate to TokenBucket for rate limiting
        allowed = await self._rate_limiter.consume(disable_limits=disable_tx_limits)

        if not allowed:
            _LOGGER.warning(f"{self}: Discarding write (tokens={self._rate_limiter!r})")
            return

        await super().write_frame(frame)

    async def _write_frame(self, frame: str) -> None:
        data = json.dumps({"msg": frame})
        if DBG_FORCE_FRAME_LOGGING:
            _LOGGER.warning("Tx: %s", data)
        elif _LOGGER.getEffectiveLevel() == logging.INFO:
            _LOGGER.info("Tx: %s", data)

        try:
            self._publish(data)
        except MQTTException as err:
            _LOGGER.error(f"MQTT publish failed: {err}")
            return

    def _publish(self, payload: str) -> None:
        if not self._connected:
            _LOGGER.debug("Cannot publish - MQTT not connected")
            return

        info: mqtt.MQTTMessageInfo = self.client.publish(
            self._topic_pub, payload=payload, qos=self._mqtt_qos
        )

        if not info:
            _LOGGER.warning("MQTT publish returned no info")
        elif info.rc != mqtt.MQTT_ERR_SUCCESS:
            _LOGGER.warning(f"MQTT publish failed with code: {info.rc}")
            if info.rc in (mqtt.MQTT_ERR_NO_CONN, mqtt.MQTT_ERR_CONN_LOST):
                self._connected = False
                if not self._closing:
                    self._schedule_reconnect()

    def _close(self, exc: exc.RamsesException | None = None) -> None:
        super()._close(exc)
        if self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None

        if not self._connected:
            return
        self._connected = False

        try:
            self.client.unsubscribe(self._topic_sub)
            self.client.disconnect()
            self.client.loop_stop()
        except Exception as err:
            _LOGGER.debug(f"Error during MQTT cleanup: {err}")
