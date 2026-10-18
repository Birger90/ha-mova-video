"""Per-robot live video lifecycle: runtime, provisioning, worker, relay."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

from .bridge import VacuumBridge
from .const import DOMAIN
from .provision import ProvisioningError, async_provision
from .relay import FlvRelay
from .xp2p.bootstrap import ensure_xp2p_host_runtime
from .xp2p.common import Xp2pHostAssets, safe_output_preview
from .xp2p.worker import StreamSession, start_stream

_LOGGER = logging.getLogger(__name__)
_RUNTIME_LOCK = asyncio.Lock()
WARM_GRACE = 180.0


class VideoManager:
    """Owns at most one XP2P worker for one robot.

    The native start can take minutes, while Home Assistant's stream worker gives up on a
    silent source after about 30 s and retries. The start therefore runs as its own task
    that survives a relay cancellation; a retry joins the running start instead of
    launching a second worker.
    """

    def __init__(self, hass: HomeAssistant, bridge: VacuumBridge, *, device_handshake: bool) -> None:
        self.hass = hass
        self.bridge = bridge
        self.device_handshake = device_handshake
        self.relay = FlvRelay(self._async_open_source, self._async_close_source)
        self._assets: Xp2pHostAssets | None = None
        self._session: StreamSession | None = None
        self._starting: asyncio.Task[StreamSession] | None = None
        self._handshake_sent = False
        self._redact: tuple[str, ...] = ()
        self._worker_tail: list[bytes] = []
        self._phase = "idle"
        self._phase_since = time.monotonic()
        self.last_report: dict[str, Any] | None = None
        self.last_error: str | None = None

    @property
    def runtime_root(self) -> Path:
        return Path(self.hass.config.path(".storage", DOMAIN, "xp2p-runtime"))

    def _set_phase(self, phase: str) -> None:
        self._phase = phase
        self._phase_since = time.monotonic()
        _LOGGER.debug("Live video phase: %s", phase)

    async def async_setup(self) -> None:
        await self.relay.async_start()

    async def async_shutdown(self) -> None:
        await self.relay.async_stop()
        starting, self._starting = self._starting, None
        if starting is not None and not starting.done():
            starting.cancel()
        await self._async_stop_session()

    async def async_runtime(self) -> Xp2pHostAssets:
        async with _RUNTIME_LOCK:
            if self._assets is None:
                self._assets = await self.hass.async_add_executor_job(ensure_xp2p_host_runtime, self.runtime_root)
            return self._assets

    # ---------- relay callbacks ----------

    async def _async_open_source(self) -> str:
        if self._session is not None and self._session.alive():
            return self._session.stream_url
        if self._starting is None or self._starting.done():
            self._starting = self.hass.async_create_background_task(
                self._async_start_worker(), f"{DOMAIN} xp2p start"
            )
        session = await asyncio.shield(self._starting)
        return session.stream_url

    async def _async_close_source(self) -> None:
        # Called when the relay pump ends. A start that is still running keeps going so a
        # retry can use it; _async_start_worker stops it if nobody is watching by then.
        if self._starting is not None and not self._starting.done():
            return
        await self._async_stop_session()

    # ---------- worker ----------

    async def _async_start_worker(self) -> StreamSession:
        self.last_error = None
        try:
            self._set_phase("runtime")
            assets = await self.async_runtime()
            self._set_phase("provisioning")
            request, report = await async_provision(self.bridge)
            self.last_report = report
            self._redact = request.sensitive() + tuple(
                v for v in (request.product_id, request.device_name, request.channel_id, request.did) if v
            )
            if self.device_handshake:
                self._set_phase("device_handshake")
                await self._async_handshake("start")
            self._set_phase("xp2p_connect")
            self._worker_tail = []
            session = await self.hass.async_add_executor_job(start_stream, assets, request, self._worker_tail)
        except ProvisioningError as err:
            self.last_report = err.report
            self._fail(str(err))
            await self._async_handshake_end()
            raise
        except asyncio.CancelledError:
            self._set_phase("cancelled")
            await self._async_handshake_end()
            raise
        except Exception as err:
            self._fail(str(err) or type(err).__name__)
            await self._async_handshake_end()
            raise
        self._session = session
        self._set_phase("streaming")
        _LOGGER.info("Live video worker ready (link mode %s)", session.link_mode)
        if not self.relay.active:
            # Home Assistant retries a failed stream with a growing back-off (10 s steps),
            # so keep the ready worker for a while instead of throwing the start away.
            self.hass.loop.call_later(WARM_GRACE, self._schedule_stop_if_unused, session)
        return session

    def _schedule_stop_if_unused(self, session: StreamSession) -> None:
        if self._session is session and not self.relay.active:
            _LOGGER.debug("Ready worker was not picked up; stopping it")
            self.hass.async_create_task(self._async_stop_session())

    def _fail(self, message: str) -> None:
        self.last_error = safe_output_preview(message, self._redact)
        self._set_phase("failed")
        _LOGGER.warning("Live video start failed: %s", self.last_error)

    async def _async_stop_session(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            await self.hass.async_add_executor_job(session.stop)
            _LOGGER.info("Live video worker stopped")
            if self._phase == "streaming":
                self._set_phase("idle")
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

    # ---------- diagnostics ----------

    def worker_status(self) -> dict[str, Any]:
        """Value-free progress of the current or last start (worker output is redacted)."""
        return {
            "phase": self._phase,
            "phase_seconds": round(time.monotonic() - self._phase_since, 1),
            "starting": self._starting is not None and not self._starting.done(),
            "worker_alive": self._session is not None and self._session.alive(),
            "worker_output": safe_output_preview(b"".join(self._worker_tail[-15:]), self._redact),
        }

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
        result["worker"] = self.worker_status()
        result["relay"] = {"active": self.relay.active, "last_error": self.relay.last_error}
        result["last_error"] = self.last_error
        return result
