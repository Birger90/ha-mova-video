"""Live video camera entity."""

from __future__ import annotations

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.components.ffmpeg import async_get_image
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_DREAME_ENTRY, DOMAIN
from .manager import VideoManager


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    manager: VideoManager = hass.data[DOMAIN][entry.entry_id]
    registry = dr.async_get(hass)
    devices = dr.async_entries_for_config_entry(registry, entry.data[CONF_DREAME_ENTRY])
    identifiers = devices[0].identifiers if devices else {(DOMAIN, entry.entry_id)}
    async_add_entities([MovaLiveCamera(manager, entry, identifiers)])


class MovaLiveCamera(Camera):
    """Tencent XP2P live view, started on demand when someone opens the stream."""

    _attr_has_entity_name = True
    _attr_translation_key = "live_video"
    _attr_supported_features = CameraEntityFeature.STREAM
    _attr_should_poll = False

    def __init__(self, manager: VideoManager, entry: ConfigEntry, identifiers: set) -> None:
        super().__init__()
        self._manager = manager
        self._attr_unique_id = f"{entry.entry_id}_live_video"
        self._attr_device_info = DeviceInfo(identifiers=identifiers)
        self.content_type = "image/jpeg"

    @property
    def is_streaming(self) -> bool:
        return self._manager.relay.active

    @property
    def extra_state_attributes(self) -> dict:
        return {"last_error": self._manager.last_error or self._manager.relay.last_error}

    async def stream_source(self) -> str | None:
        return self._manager.relay.url

    async def async_camera_image(self, width: int | None = None, height: int | None = None) -> bytes | None:
        # Only grab stills while someone is already watching; dashboard thumbnails must not
        # wake the camera every few seconds.
        url = self._manager.relay.url
        if url is None or not self._manager.relay.active:
            return None
        return await async_get_image(self.hass, url, width=width, height=height)
