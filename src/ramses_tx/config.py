from dataclasses import dataclass, field
from typing import Any, cast

from .const import (
    DEFAULT_DISABLE_QOS,
    SZ_ACTIVE_HGI,
    SZ_DISABLE_QOS,
    SZ_DISABLE_SENDING,
    SZ_ENFORCE_KNOWN_LIST,
    SZ_LOG_ALL_MQTT,
    SZ_SQLITE_INDEX,
)
from .schemas import select_device_filter_mode
from .typing import DeviceListT, PktLogConfigT, PortConfigT


@dataclass(frozen=True)
class GatewayConfig:
    """Configuration for the RAMSES RF Gateway."""

    port_name: str | None = None
    port_config: PortConfigT = field(default_factory=lambda: cast(PortConfigT, {}))
    packet_log: PktLogConfigT = field(default_factory=lambda: cast(PktLogConfigT, {}))

    # Device Lists
    block_list: DeviceListT = field(default_factory=dict)
    known_list: DeviceListT = field(default_factory=dict)

    # Flags
    disable_discovery: bool | str = False
    disable_qos: bool | None = DEFAULT_DISABLE_QOS
    disable_sending: bool = False
    enable_eavesdrop: bool = False
    enforce_known_list: bool = False
    log_all_mqtt: bool = False
    reduce_processing: int = 0
    use_native_ot: bool = False  # Prefer native OpenTherm decoding
    use_sqlite_index: bool = False  # TODO Q1 2026: default True

    # Misc
    evofw_flag: str | None = None
    hgi_id: str | None = None

    @classmethod
    def from_kwargs(
        cls, port_name: str | None, input_file: str | None, **kwargs: Any
    ) -> "GatewayConfig":
        """Factory to create config from legacy kwargs (transition helper)."""

        # Handle the specific logic for disable_sending regarding input_file
        disable_sending = kwargs.pop(SZ_DISABLE_SENDING, False)
        if input_file:
            disable_sending = True

        enforce_known = select_device_filter_mode(
            kwargs.pop(SZ_ENFORCE_KNOWN_LIST, None),
            kwargs.get("known_list", {}),
            kwargs.get("block_list", {}),
        )

        return cls(
            port_name=port_name,
            port_config=cast(PortConfigT, kwargs.pop("port_config", {})),
            packet_log=cast(PktLogConfigT, kwargs.pop("packet_log", {})),
            block_list=kwargs.get("block_list", {}),
            known_list=kwargs.get("known_list", {}),
            # Flags
            disable_discovery=kwargs.pop("disable_discovery", False),
            disable_qos=kwargs.pop(SZ_DISABLE_QOS, DEFAULT_DISABLE_QOS),
            disable_sending=disable_sending,
            enable_eavesdrop=kwargs.pop("enable_eavesdrop", False),
            enforce_known_list=enforce_known,
            log_all_mqtt=kwargs.pop(SZ_LOG_ALL_MQTT, False),
            reduce_processing=kwargs.pop("reduce_processing", 0),
            use_native_ot=kwargs.pop("use_native_ot", False),
            use_sqlite_index=kwargs.pop(SZ_SQLITE_INDEX, False),
            # Misc
            evofw_flag=kwargs.get("evofw_flag"),
            hgi_id=kwargs.get(SZ_ACTIVE_HGI) or kwargs.get("hgi_id"),
        )
