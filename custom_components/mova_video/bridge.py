"""Access to the authenticated cloud session of the Tasshack dreame_vacuum integration.

This integration does not log in on its own. It reuses the cloud protocol object that
dreame_vacuum already holds, so no credentials are stored here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from homeassistant.core import HomeAssistant

from .const import DREAME_DOMAIN, TX_PATHS

_LOGGER = logging.getLogger(__name__)


class BridgeError(Exception):
    """Raised when the dreame_vacuum session is not usable."""


@dataclass
class VacuumBridge:
    """Thin wrapper around dreame_vacuum's coordinator, device and cloud objects."""

    hass: HomeAssistant
    dreame_entry_id: str

    def _coordinator(self) -> Any:
        coordinators = self.hass.data.get(DREAME_DOMAIN) or {}
        coordinator = coordinators.get(self.dreame_entry_id)
        if coordinator is None:
            raise BridgeError("dreame_vacuum entry is not loaded")
        return coordinator

    def device(self) -> Any:
        coordinator = self._coordinator()
        device = getattr(coordinator, "device", None) or getattr(coordinator, "_device", None)
        if device is None:
            raise BridgeError("dreame_vacuum device is not initialised")
        return device

    def cloud(self) -> Any:
        protocol = getattr(self.device(), "_protocol", None)
        cloud = getattr(protocol, "cloud", None)
        if cloud is None:
            raise BridgeError("dreame_vacuum has no cloud connection")
        if not (hasattr(cloud, "request") and hasattr(cloud, "get_api_url")):
            raise BridgeError("dreame_vacuum cloud object has no request() (unsupported version)")
        return cloud

    def identity(self) -> dict[str, Any]:
        cloud = self.cloud()
        return {
            "did": str(getattr(cloud, "_did", "") or ""),
            "uid": str(getattr(cloud, "_uid", "") or ""),
            "model": str(getattr(cloud, "_model", "") or ""),
        }

    def capabilities(self) -> dict[str, bool]:
        cap = getattr(self.device(), "capability", None)
        names = ("camera_streaming", "stream_tx", "stream_agora", "stream_ali", "video_audio", "video_during_cleaning")
        return {n: bool(getattr(cap, n, False)) for n in names}

    def stream_session(self) -> Any:
        status = getattr(self.device(), "status", None)
        return getattr(status, "stream_session", None)

    async def async_tx_call(self, kind: str, params: dict[str, Any]) -> Any:
        """POST one of the /dreame-third-video/tx/ endpoints through dreame_vacuum's signed client."""
        cloud = self.cloud()
        url = f"{cloud.get_api_url()}{TX_PATHS[kind]}"
        body = json.dumps(params, separators=(",", ":"))
        # Same call shape as the vendor-app port in the mower integration: plain JSON body,
        # authentication headers added by dreame_vacuum's request().
        return await self.hass.async_add_executor_job(cloud.request, url, body)

    async def async_stream_video_action(self, oper_type: str, operation: str = "monitor") -> Any:
        """Send the device-side stream status action (siid 10001 / aiid 1) like the vendor app."""
        device = self.device()
        types = __import__(
            f"custom_components.{DREAME_DOMAIN}.dreame.types", fromlist=["DreameVacuumProperty"]
        )
        prop = types.DreameVacuumProperty.STREAM_STATUS
        return await self.hass.async_add_executor_job(
            device.call_stream_video_action, prop, {"operType": oper_type, "operation": operation}
        )
