"""Per-robot live video lifecycle: runtime, provisioning, worker, relay."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

from .bridge import VacuumBridge
from .const import DOMAIN
from .provision import ProvisioningError, async_provision
from .relay import FlvRelay
from .xp2p.bootstrap import ensure_xp2p_host_runtime
from .xp2p.common import Xp2pHostAssets
from .xp2p.worker import StreamSession, start_stream

_LOGGER = logging.getLogger(__name__)
_RUNTIME_LOCK = asyncio.Lock()


class VideoManager:
    """Owns at most one XP2P worker for one robot."""

    def __init__(self, hass: HomeAssistant, bridge: VacuumBridge, *, device_handshake: bool) -> None:
        self.hass = hass
        self.bridge = bridge
        self.device_handshake = device_handshake
        self.relay = FlvRelay(self._async_open_source, self._async_close_source)
        self._assets: Xp2pHostAssets | None = None
        self._session: StreamSession | None = None
        self._handshake_sent = False
        self.last_report: dict[str, Any] | None = None
        self.last_error: str | None = None

    @property
    def runtime_root(self) -> Path:
        return Path(self.hass.config.path(".storage", DOMAIN, "xp2p-runtime"))

    async def async_setup(self) -> None:
        await self.relay.async_start()

    async def async_shutdown(self) -> None:
        await self.relay.async_stop()
        await self._async_close_source()

    async def async_runtime(self) -> Xp2pHostAssets:
        async with _RUNTIME_LOCK:
            if self._assets is None:
                self._assets = await self.hass.async_add_executor_job(ensure_xp2p_host_runtime, self.runtime_root)
            return self._assets

    async def _async_open_source(self) -> str:
        self.last_error = None
        try:
            assets = await self.async_runtime()
            request, report = await async_provision(self.bridge)
            self.last_report = report
            if self.device_handshake:
                await self._async_handshake("start")
            self._session = await self.hass.async_add_executor_job(start_stream, assets, request)
            _LOGGER.info("Live video started (link mode %s)", self._session.link_mode)
            return self._session.stream_url
        except ProvisioningError as err:
            self.last_report = err.report
            self.last_error = str(err)
            await self._async_handshake_end()
            raise
        except Exception as err:
            self.last_error = str(err) or type(err).__name__
            await self._async_handshake_end()
            raise

    async def _async_close_source(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            await self.hass.async_add_executor_job(session.stop)
            _LOGGER.info("Live video worker stopped")
        await self._async_handshake_end()

    async def _async_handshake(self, oper_type: str) -> None:
        try:
            result = await self.bridge.async_stream_video_action(oper_type)
            if oper_type == "start":
                self._handshake_sent = True
            _LOGGER.debug("Stream video action %s -> %s", oper_type, (result or {}).get("code") if isinstance(result, dict) else type(result).__name__)
        except Exception as err:  # noqa: BLE001 - best effort, unverified for this model
            _LOGGER.debug("Stream video action %s failed: %s", oper_type, err)

    async def _async_handshake_end(self) -> None:
        if self._handshake_sent:
            self._handshake_sent = False
            await self._async_handshake("end")

    async def async_diagnose(self, *, start_runtime: bool) -> dict[str, Any]:
        """Value-free check of capabilities, cloud provisioning and (optionally) the runtime."""
        result: dict[str, Any] = {"capabilities": {}, "identity": {}}
        try:
            result["capabilities"] = self.bridge.capabilities()
            ident = self.bridge.identity()
            result["identity"] = {"did_present": bool(ident["did"]), "uid_present": bool(ident["uid"]), "model": ident["model"]}
            result["stream_session_present"] = self.bridge.stream_session() is not None
        except Exception as err:  # noqa: BLE001
            result["bridge_error"] = str(err)
            return result
        try:
            request, report = await async_provision(self.bridge)
            result["provisioning"] = {"ready": True, **report}
            del request
        except ProvisioningError as err:
            result["provisioning"] = {"ready": False, "error": str(err), **err.report}
        except Exception as err:  # noqa: BLE001
            result["provisioning"] = {"ready": False, "error": f"{type(err).__name__}: {err}"}
        if start_runtime:
            try:
                assets = await self.async_runtime()
                result["runtime"] = {"ready": True, "probe": assets.startup_probe}
            except Exception as err:  # noqa: BLE001
                result["runtime"] = {"ready": False, "error": f"{type(err).__name__}: {err}"}
        result["relay"] = {"active": self.relay.active, "last_error": self.relay.last_error}
        result["last_error"] = self.last_error
        return result
