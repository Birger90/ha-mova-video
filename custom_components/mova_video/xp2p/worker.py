"""Drive the prebuilt Bionic XP2P worker (blocking; run in an executor).

Wire protocol, field order and defaults follow EvotecIT/homeassistant-dreamelawnmower
(dreame_lawn_mower_client/xp2p_host_runtime.py, xp2p_config.py; MIT License).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import queue
import struct
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import requests

from .common import VideoRuntimeError, Xp2pHostAssets, safe_output_preview

_LOGGER = logging.getLogger(__name__)

_REQUEST_MAGIC = b"DXP1"
_RESPONSE_MAGIC = b"DXR1"
_REQUEST_FIELDS = 20
_MAX_RESPONSE_LENGTH = 64 * 1024
COMMAND_TIMEOUT_US = 7_500_000
STATUS_ATTEMPTS = 60
RETRY_INTERVAL = 0.5
DEFAULT_STUN_SERVER = "43.158.113.38:20002"
DEFAULT_STUN_PORT = 20002
PROTOCOL_AUTO, PROTOCOL_UDP, PROTOCOL_TCP = 0, 1, 2
TENCENT_APP_API = "https://iot.cloud.tencent.com/api/exploreropen/appapi"
FLV_PATH_TEMPLATE = "ipc.flv?action=live&channel={channel}&quality=high&_crypto=on"
DEVICE_STATUS_COMMAND = "action=inner_define&channel=0&cmd=get_device_st&type=live&quality=standard"
LIVE_COMMAND = "action=live"

WORKER_ERRORS = {
    1: "Invalid XP2P worker request.",
    2: "Could not load the XP2P runtime.",
    3: "The XP2P runtime is missing required symbols.",
    10: "XP2P QCloud credential setup failed.",
    11: "XP2P service startup failed.",
    12: "XP2P device configuration failed.",
    13: "XP2P device status command failed.",
    14: "The robot XP2P channel did not become ready.",
    15: "XP2P AV receive startup failed.",
    16: "XP2P did not expose a local FLV URL.",
    17: "XP2P did not reach its native ready event.",
}


@dataclass(slots=True)
class StreamRequest:
    """Normalised provisioning needed by the worker. Holds secrets: never log."""

    product_id: str
    device_name: str
    p2p_info: str
    secret_id: str = ""
    secret_key: str = ""
    channel_id: str = ""
    app_id: str = ""
    app_secret: str = ""
    did: str = ""
    channel: int = 0

    @property
    def service_id(self) -> str:
        return f"{self.product_id}/{self.device_name}"

    @property
    def delegate_id(self) -> str:
        return self.channel_id or self.service_id

    @property
    def flv_path(self) -> str:
        return FLV_PATH_TEMPLATE.format(channel=self.channel)

    def sensitive(self) -> tuple[str, ...]:
        return tuple(
            v for v in (self.p2p_info, self.secret_id, self.secret_key, self.app_secret, self.app_id) if v
        )


@dataclass(slots=True)
class DeviceConfig:
    server: str = ""
    ip: str = ""
    port: int = DEFAULT_STUN_PORT
    protocol_type: int = PROTOCOL_TCP
    cross: bool = False


@dataclass
class StreamSession:
    """A running worker that exposes a single-consumer local HTTP-FLV URL."""

    stream_url: str
    process: subprocess.Popen
    stun_file: Path | None
    stderr_thread: threading.Thread | None
    link_mode: int | None = None
    stderr_tail: list[bytes] = field(default_factory=list)

    def alive(self) -> bool:
        return self.process.poll() is None

    def stop(self) -> None:
        proc = self.process
        try:
            if proc.stdin is not None and proc.poll() is None:
                try:
                    proc.stdin.write(b"\0")
                    proc.stdin.flush()
                except OSError:
                    pass
                try:
                    proc.stdin.close()
                except OSError:
                    pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
        finally:
            if self.stun_file is not None:
                self.stun_file.unlink(missing_ok=True)


# ---------- Tencent device configuration (optional, falls back to SDK defaults) ----------

def _sign(parameters: dict[str, Any], app_secret: str) -> str:
    parts = []
    for key in sorted(k for k in parameters if k != "Signature"):
        value = parameters[key]
        if value is not None and str(value):
            parts.append(f"{key.replace('_', '.')}={value}")
    digest = hmac.new(app_secret.encode(), "&".join(parts).encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode("ascii")


def fetch_device_config(req: StreamRequest, timeout: float = 10.0) -> DeviceConfig:
    """Ask Tencent for STUN/protocol settings; any failure returns the SDK defaults."""
    if not (req.app_id and req.app_secret and req.product_id and req.device_name):
        return DeviceConfig()
    params: dict[str, Any] = {
        "Action": "AppDescribeConfigureDeviceP2P",
        "Timestamp": int(time.time()),
        "Nonce": int(time.time() * 1000),
        "AppKey": req.app_id,
        "ProductId": req.product_id,
        "DeviceName": req.device_name,
        "RequestId": str(uuid.uuid4()),
    }
    params["Signature"] = _sign(params, req.app_secret)
    try:
        resp = requests.post(TENCENT_APP_API, json=params, timeout=timeout, allow_redirects=False)
        payload = resp.json() if resp.status_code == 200 else None
    except (requests.RequestException, ValueError):
        payload = None
    if not isinstance(payload, dict) or payload.get("code") not in (0, "0"):
        return DeviceConfig()
    cfg = (payload.get("data") or {}).get("Config")
    if not isinstance(cfg, dict):
        return DeviceConfig()
    try:
        port = int(cfg.get("StunPort") or DEFAULT_STUN_PORT)
    except (TypeError, ValueError):
        port = DEFAULT_STUN_PORT
    protocol = {"AUTO": PROTOCOL_AUTO, "TCP": PROTOCOL_TCP, "UDP": PROTOCOL_UDP}.get(
        str(cfg.get("Protocol") or "").upper(), PROTOCOL_TCP
    )
    return DeviceConfig(
        server=str(cfg.get("StunHost") or "").strip(),
        ip=str(cfg.get("StunIP") or "").strip(),
        port=port if 0 < port <= 65535 else DEFAULT_STUN_PORT,
        protocol_type=protocol,
        cross=str(cfg.get("EnableCrossStunTurn")) == "1",
    )


def _write_stun_file(cfg: DeviceConfig) -> Path:
    servers = tuple(dict.fromkeys(f"{h}:{cfg.port}" for h in (cfg.server, cfg.ip) if h)) or (
        DEFAULT_STUN_SERVER,
    )
    handle = tempfile.NamedTemporaryFile(
        "w", delete=False, encoding="utf-8", prefix="mova-xp2p-stun-", suffix=".txt"
    )
    with handle:
        for server in servers:
            handle.write(server + "\n")
    return Path(handle.name)


# ---------- wire protocol ----------

def _encode_request(assets: Xp2pHostAssets, stun_file: Path, req: StreamRequest, cfg: DeviceConfig) -> bytes:
    fields = (
        str(assets.library_path),
        str(stun_file),
        req.service_id,
        req.delegate_id,
        req.product_id,
        req.device_name,
        req.p2p_info,
        req.secret_id or "",
        req.secret_key or "",
        req.flv_path,
        DEVICE_STATUS_COMMAND,
        LIVE_COMMAND,
        cfg.server,
        cfg.ip,
        str(cfg.port),
        str(cfg.protocol_type),
        "1" if cfg.cross else "0",
        "cloud",
        "",
        "",
    )
    payload = bytearray(_REQUEST_MAGIC)
    payload.extend(struct.pack("!I", _REQUEST_FIELDS))
    for value in fields:
        raw = value.encode("utf-8")
        payload.extend(struct.pack("!I", len(raw)))
        payload.extend(raw)
    payload.extend(
        struct.pack(
            "!QIIII",
            COMMAND_TIMEOUT_US,
            STATUS_ATTEMPTS,
            int(RETRY_INTERVAL * 1000),
            STATUS_ATTEMPTS,
            int(RETRY_INTERVAL * 1000),
        )
    )
    return bytes(payload)


def _startup_timeout() -> float:
    command_timeout = COMMAND_TIMEOUT_US / 1_000_000
    return max(
        45.0,
        STATUS_ATTEMPTS * RETRY_INTERVAL
        + STATUS_ATTEMPTS * (command_timeout + RETRY_INTERVAL)
        + STATUS_ATTEMPTS * RETRY_INTERVAL
        + 5.0,
    )


def _read_exact(stream: IO[bytes], length: int) -> bytes:
    result = bytearray()
    while len(result) < length:
        chunk = stream.read(length - len(result))
        if not chunk:
            raise VideoRuntimeError("XP2P worker closed its output.")
        result.extend(chunk)
    return bytes(result)


def _read_response(stream: IO[bytes], write: Callable[[], None], timeout: float) -> tuple[int, bytes]:
    box: queue.Queue = queue.Queue(maxsize=1)

    def run() -> None:
        try:
            write()
            header = _read_exact(stream, 12)
            if header[:4] != _RESPONSE_MAGIC:
                raise VideoRuntimeError("XP2P worker returned an invalid response.")
            status, length = struct.unpack("!II", header[4:])
            if length > _MAX_RESPONSE_LENGTH:
                raise VideoRuntimeError("XP2P worker response is too large.")
            box.put((status, _read_exact(stream, length)))
        except BaseException as err:  # noqa: BLE001
            box.put(err)

    threading.Thread(target=run, name="mova-xp2p-response", daemon=True).start()
    try:
        value = box.get(timeout=timeout)
    except queue.Empty as err:
        raise VideoRuntimeError(f"XP2P worker timed out after {timeout:.0f}s.") from err
    if isinstance(value, BaseException):
        raise value
    return value


def _drain(stream: IO[bytes] | None, tail: list[bytes]) -> threading.Thread | None:
    if stream is None:
        return None

    def run() -> None:
        for line in iter(stream.readline, b""):
            tail.append(line[-400:])
            del tail[:-40]

    thread = threading.Thread(target=run, name="mova-xp2p-stderr", daemon=True)
    thread.start()
    return thread


def start_stream(assets: Xp2pHostAssets, req: StreamRequest) -> StreamSession:
    """Launch the worker and return the local FLV URL. Blocks up to several minutes."""
    assets.validate()
    cfg = fetch_device_config(req)
    stun_file = _write_stun_file(cfg)
    try:
        process = subprocess.Popen(
            assets.command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=assets.environment(),
        )
    except OSError as err:
        stun_file.unlink(missing_ok=True)
        raise VideoRuntimeError(f"Could not start the XP2P worker: {err}") from err

    tail: list[bytes] = []
    drain = _drain(process.stderr, tail)
    session = StreamSession("", process, stun_file, drain, stderr_tail=tail)
    try:
        payload = _encode_request(assets, stun_file, req, cfg)
        stdin = process.stdin

        def write() -> None:
            stdin.write(payload)
            stdin.flush()

        status, body = _read_response(process.stdout, write, _startup_timeout())
        text = body.decode("utf-8", "replace")
        if status != 0:
            native = safe_output_preview(b"".join(tail), req.sensitive())
            message = WORKER_ERRORS.get(status, f"XP2P worker failed (status {status}).")
            detail = safe_output_preview(text, req.sensitive())
            raise VideoRuntimeError(" ".join(p for p in (message, detail, f"Native: {native}" if native else "") if p))
        first, sep, rest = text.partition("\n")
        if sep and first in {"0", "62", "63"}:
            session.link_mode = int(first) or None
            text = rest
        if not text.startswith(("http://", "https://")):
            raise VideoRuntimeError("XP2P worker returned an invalid stream URL.")
        if process.poll() is not None:
            raise VideoRuntimeError("XP2P worker exited before the stream could be used.")
        session.stream_url = text.strip()
        return session
    except BaseException:
        session.stop()
        raise
