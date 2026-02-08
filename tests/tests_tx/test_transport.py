#!/usr/bin/env python3
"""Tests for CallbackTransport initialization logic."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ramses_tx import exceptions as exc
from ramses_tx.transports import CallbackTransport, is_hgi80, transport_factory


async def _async_callback_factory(*args: Any, **kwargs: Any) -> CallbackTransport:
    """Async wrapper for CallbackTransport to satisfy transport_factory signature."""
    return CallbackTransport(*args, **kwargs)


async def test_callback_transport_handshake() -> None:
    """Test that connection_made is called automatically upon initialization."""
    mock_protocol = Mock()
    mock_writer = AsyncMock()

    transport = CallbackTransport(mock_protocol, mock_writer)

    # Assert handshake called immediately
    mock_protocol.connection_made.assert_called_once_with(transport, ramses=True)


async def test_callback_transport_handshake_idempotency() -> None:
    """Test that manual connection_made calls are safe (idempotent at protocol level)."""
    mock_protocol = Mock()
    mock_writer = AsyncMock()

    transport = CallbackTransport(mock_protocol, mock_writer)

    # Verify initial call
    mock_protocol.connection_made.assert_called_once()

    # Manually call again (simulating legacy consumer behavior)
    mock_protocol.connection_made(transport, ramses=True)

    # Assert called twice without error (protocol impl handles idempotency logic)
    assert mock_protocol.connection_made.call_count == 2


async def test_callback_transport_autostart_false() -> None:
    """Test that reading is paused by default (autostart=False)."""
    mock_protocol = Mock()
    mock_writer = AsyncMock()

    transport = CallbackTransport(mock_protocol, mock_writer, autostart=False)

    assert transport.is_reading() is False


async def test_callback_transport_autostart_default() -> None:
    """Test that reading is paused by default (backward compatibility)."""
    mock_protocol = Mock()
    mock_writer = AsyncMock()

    transport = CallbackTransport(mock_protocol, mock_writer)

    assert transport.is_reading() is False


async def test_callback_transport_autostart_true() -> None:
    """Test that reading is resumed automatically if autostart=True."""
    mock_protocol = Mock()
    mock_writer = AsyncMock()

    transport = CallbackTransport(mock_protocol, mock_writer, autostart=True)

    assert transport.is_reading() is True


async def test_factory_routes_autostart_to_custom_constructor() -> None:
    """Check that autostart is passed to a custom transport_constructor."""
    mock_protocol = Mock()
    mock_writer = AsyncMock()

    # 1. Test with autostart=True
    # NOTE: transport_factory awaits the constructor, so we must pass an async callable
    transport = await transport_factory(
        mock_protocol,
        transport_constructor=_async_callback_factory,
        io_writer=mock_writer,
        autostart=True,
    )
    assert isinstance(transport, CallbackTransport)
    assert transport.is_reading() is True

    # 2. Test with autostart=False (default)
    transport_paused = await transport_factory(
        mock_protocol,
        transport_constructor=_async_callback_factory,
        io_writer=mock_writer,
        autostart=False,
    )
    assert isinstance(transport_paused, CallbackTransport)
    assert transport_paused.is_reading() is False


async def test_factory_strips_autostart_for_standard_transport() -> None:
    """Check that autostart is REMOVED before calling standard transports.

    If it isn't removed, the standard transports (PortTransport/MqttTransport)
    would raise TypeError because they don't accept 'autostart' in __init__.
    """
    mock_protocol = Mock()
    mock_protocol.wait_for_connection_made = AsyncMock()

    # REF: We patch 'ramses_tx.transports.PortTransport' because transport_factory
    # imports it directly into the 'ramses_tx.transports' namespace.
    with (
        patch("ramses_tx.transports.PortTransport") as MockPortTransport,
        patch("ramses_tx.transports.serial.serial_for_url") as mock_serial_for_url,
    ):
        # Setup the mock serial object to pass validity checks
        mock_serial = Mock()
        mock_serial.portstr = "/dev/ttyUSB0"
        mock_serial_for_url.return_value = mock_serial

        # valid-looking config so factory enters the Serial branch
        port_config: Any = {}

        await transport_factory(
            mock_protocol,
            port_name="/dev/ttyUSB0",
            port_config=port_config,
            autostart=True,  # This argument must be filtered out!
        )

        # Assert PortTransport was called
        assert MockPortTransport.call_count == 1

        # Verify 'autostart' was NOT in the call args
        call_args = MockPortTransport.call_args
        assert "autostart" not in call_args.kwargs
        assert "autostart" not in call_args.args  # just in case


async def test_factory_strips_autostart_for_mqtt_transport() -> None:
    """Check that autostart is REMOVED before calling MqttTransport."""
    mock_protocol = Mock()
    mock_protocol.wait_for_connection_made = AsyncMock()

    # REF: Patch 'ramses_tx.transports.MqttTransport' for the same reason as above.
    with patch("ramses_tx.transports.MqttTransport") as MockMqttTransport:
        # valid-looking config so factory enters the MQTT branch
        # We must provide port_config because transport_factory validates it
        # is not None even for MQTT
        port_config: Any = {}

        await transport_factory(
            mock_protocol,
            port_name="mqtt://broker:1883",
            port_config=port_config,
            autostart=True,  # This must be filtered out
        )

        assert MockMqttTransport.call_count == 1
        call_args = MockMqttTransport.call_args
        assert "autostart" not in call_args.kwargs


async def test_port_transport_close_robustness() -> None:
    """Check that PortTransport.close() does not raise AttributeError if init failed.

    This ensures that _close() checks for the existence of _init_task before
    attempting to cancel it.
    """

    # 1. Define a Dummy class to act as SerialTransport.
    #    We DO NOT use Mock() here because patching Mock.__init__ breaks everything.
    class DummySerialTransport:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    # 2. Mock the missing module 'serial_asyncio'
    mock_serial_asyncio = Mock()
    mock_serial_asyncio.SerialTransport = DummySerialTransport

    with patch.dict("sys.modules", {"serial_asyncio": mock_serial_asyncio}):
        # Now it is safe to import.
        from ramses_tx.transports.serial import PortTransport

        mock_protocol = Mock()
        mock_serial = Mock()

        # Define a side_effect for SerialTransport.__init__
        def mock_init(
            self: Any, loop: Any, protocol: Any, serial_instance: Any
        ) -> None:
            self._loop = loop or asyncio.get_event_loop()
            self._protocol = protocol
            # Set BOTH backing attribute and public attribute to be safe
            self._serial = serial_instance
            self.serial = serial_instance

        # Patch the Dummy class's __init__
        with patch.object(
            DummySerialTransport,
            "__init__",
            side_effect=mock_init,
            autospec=True,
        ):
            transport = PortTransport(mock_serial, mock_protocol)

            # Pre-condition check
            assert not hasattr(transport, "_init_task")

            # Execute close - should not raise AttributeError
            transport.close()


async def test_is_hgi80_async_file_check() -> None:
    """Check that is_hgi80 uses loop.run_in_executor for file existence checks.

    This test verifies that the file check does not block the event loop and
    uses the expected executor logic.
    """
    test_port = "/dev/serial/by-id/usb-SparkFun_evofw3_TEST"

    # 1. Test: File exists (should return False due to 'evofw3' in name)
    # REF: Patch os.path.exists in the new location (transports.serial)
    with patch(
        "ramses_tx.transports.serial.os.path.exists", return_value=True
    ) as mock_exists:
        result = await is_hgi80(test_port)

        mock_exists.assert_called_once_with(test_port)
        assert result is False

    # 2. Test: File does NOT exist (should raise TransportSerialError)
    with patch(
        "ramses_tx.transports.serial.os.path.exists", return_value=False
    ) as mock_exists:
        with pytest.raises(exc.TransportSerialError):
            await is_hgi80(test_port)

        mock_exists.assert_called_once_with(test_port)
