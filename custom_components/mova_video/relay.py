"""Loopback HTTP-FLV relay: one upstream (the XP2P worker URL), many local consumers.

The Tencent FLV URL accepts a single consumer. The relay opens it lazily on the first
request, fans the bytes out, replays the FLV header plus codec configuration to late
joiners and starts them on the next key frame, and stops the upstream after an idle grace.
Concept follows EvotecIT/homeassistant-dreamelawnmower video_flv_relay.py (MIT); this is a
smaller reimplementation.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import struct
from collections.abc import Awaitable, Callable

import aiohttp
from aiohttp import web

_LOGGER = logging.getLogger(__name__)

FIRST_MEDIA_TIMEOUT = 20.0
READ_TIMEOUT = 30.0
IDLE_GRACE = 20.0
QUEUE_LIMIT = 400


class _Subscriber:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self.started = False  # becomes True at the first key frame


class FlvRelay:
    """Serve http://127.0.0.1:<port>/<token>.flv for HA stream and ffmpeg snapshots."""

    def __init__(
        self,
        source_factory: Callable[[], Awaitable[str]],
        source_closer: Callable[[], Awaitable[None]],
    ) -> None:
        self._source_factory = source_factory
        self._source_closer = source_closer
        self._token = secrets.token_urlsafe(16)
        self._runner: web.AppRunner | None = None
        self._port: int | None = None
        self._subs: set[_Subscriber] = set()
        self._pump: asyncio.Task | None = None
        self._idle: asyncio.TimerHandle | None = None
        self._init: bytes = b""
        self._init_done = False
        self._lock = asyncio.Lock()
        self.last_error: str | None = None

    @property
    def url(self) -> str | None:
        return f"http://127.0.0.1:{self._port}/{self._token}.flv" if self._port else None

    @property
    def active(self) -> bool:
        return self._pump is not None and not self._pump.done()

    async def async_start(self) -> None:
        app = web.Application()
        app.router.add_get(f"/{self._token}.flv", self._handle)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self._port = site._server.sockets[0].getsockname()[1]  # noqa: SLF001

    async def async_stop(self) -> None:
        await self._stop_upstream()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    # ---------- consumers ----------

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        sub = _Subscriber()
        async with self._lock:
            self._subs.add(sub)
            if self._idle is not None:
                self._idle.cancel()
                self._idle = None
            if not self.active:
                self._init, self._init_done = b"", False
                self._pump = asyncio.create_task(self._run_pump())
        response = web.StreamResponse(headers={"Content-Type": "video/x-flv", "Cache-Control": "no-store"})
        await response.prepare(request)
        try:
            while True:
                chunk = await sub.queue.get()
                if chunk is None:
                    break
                await response.write(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            self._subs.discard(sub)
            if not self._subs:
                loop = asyncio.get_running_loop()
                self._idle = loop.call_later(IDLE_GRACE, lambda: asyncio.ensure_future(self._idle_stop()))
        return response

    async def _idle_stop(self) -> None:
        if not self._subs:
            _LOGGER.debug("No viewers left; stopping live video upstream")
            await self._stop_upstream()

    def _broadcast(self, data: bytes, keyframe: bool) -> None:
        for sub in list(self._subs):
            if not sub.started:
                if not keyframe or not self._init_done:
                    continue
                sub.started = True
                self._put(sub, self._init)
            self._put(sub, data)

    def _put(self, sub: _Subscriber, data: bytes) -> None:
        try:
            sub.queue.put_nowait(data)
        except asyncio.QueueFull:
            # Slow consumer: drop it rather than stall everyone.
            self._subs.discard(sub)
            try:
                sub.queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    # ---------- upstream ----------

    async def _run_pump(self) -> None:
        self.last_error = None
        try:
            url = await self._source_factory()
            timeout = aiohttp.ClientTimeout(total=None, sock_read=READ_TIMEOUT, connect=15)
            async with aiohttp.ClientSession(timeout=timeout) as session, session.get(url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Upstream FLV returned HTTP {resp.status}")
                await asyncio.wait_for(self._pump_flv(resp.content), timeout=None)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            self.last_error = str(err) or type(err).__name__
            _LOGGER.warning("Live video stopped: %s", self.last_error)
        finally:
            for sub in list(self._subs):
                try:
                    sub.queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass
            await self._source_closer()

    async def _pump_flv(self, content: aiohttp.StreamReader) -> None:
        header = await asyncio.wait_for(content.readexactly(13), FIRST_MEDIA_TIMEOUT)
        if header[:3] != b"FLV":
            raise RuntimeError("Upstream is not an FLV stream")
        init = bytearray(header)
        while True:
            tag_header = await content.readexactly(11)
            tag_type = tag_header[0] & 0x1F
            size = struct.unpack(">I", b"\0" + tag_header[1:4])[0]
            body = await content.readexactly(size + 4)  # data + PreviousTagSize
            tag = tag_header + body
            keyframe = False
            if tag_type == 18 and not self._init_done:  # script/metadata
                init.extend(tag)
                continue
            if tag_type == 9 and size >= 2:  # video
                frame_type = body[0] >> 4
                codec = body[0] & 0x0F
                if codec in (7, 12) and body[1] == 0:  # AVC/HEVC sequence header
                    init.extend(tag)
                    continue
                keyframe = frame_type == 1
            elif tag_type == 8 and size >= 2 and (body[0] >> 4) == 10 and body[1] == 0:  # AAC config
                if not self._init_done:
                    init.extend(tag)
                continue
            if keyframe and not self._init_done:
                self._init = bytes(init)
                self._init_done = True
            self._broadcast(tag, keyframe)

    async def _stop_upstream(self) -> None:
        pump, self._pump = self._pump, None
        if pump is not None and not pump.done():
            pump.cancel()
            try:
                await pump
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
