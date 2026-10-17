"""MOVA Video: Tencent XP2P live view for MOVA/Dreame robot vacuums.

Companion to Tasshack/dreame-vacuum. Reuses its authenticated cloud session; stores no
credentials. The XP2P runtime and worker are taken from EvotecIT/homeassistant-dreamelawnmower
(MIT License).
"""

from __future__ import annotations

import logging

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import ConfigEntryNotReady, ServiceValidationError
from homeassistant.helpers import config_validation as cv

from .bridge import BridgeError, VacuumBridge
from .const import CONF_DREAME_ENTRY, DOMAIN, DREAME_DOMAIN
from .manager import VideoManager

_LOGGER = logging.getLogger(__name__)
PLATFORMS = [Platform.CAMERA]
CONF_HANDSHAKE = "device_handshake"
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    async def diagnose(call: ServiceCall) -> ServiceResponse:
        managers: dict = hass.data.get(DOMAIN, {})
        if not managers:
            raise ServiceValidationError("MOVA Video is not set up")
        out = {}
        for entry_id, manager in managers.items():
            out[entry_id] = await manager.async_diagnose(start_runtime=call.data.get("runtime", False))
        return out

    hass.services.async_register(
        DOMAIN,
        "diagnose",
        diagnose,
        schema=vol.Schema({vol.Optional("runtime", default=False): cv.boolean}),
        supports_response=SupportsResponse.ONLY,
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    dreame_id = entry.data[CONF_DREAME_ENTRY]
    if dreame_id not in (hass.data.get(DREAME_DOMAIN) or {}):
        raise ConfigEntryNotReady("dreame_vacuum is not loaded yet")
    bridge = VacuumBridge(hass, dreame_id)
    try:
        bridge.cloud()
    except BridgeError as err:
        raise ConfigEntryNotReady(str(err)) from err
    manager = VideoManager(hass, bridge, device_handshake=entry.options.get(CONF_HANDSHAKE, True))
    await manager.async_setup()
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = manager
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_reload))
    return True


async def _async_reload(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    manager: VideoManager | None = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if manager is not None:
        await manager.async_shutdown()
    return ok
