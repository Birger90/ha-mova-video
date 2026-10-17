"""Fetch Tencent XP2P provisioning for the robot from the Dreame/MOVA cloud.

Request parameters and response key lists mirror EvotecIT/homeassistant-dreamelawnmower
(cloud_video.py, client_camera.py _normalize_tx_rtc_info/_normalize_tx_p2p_info; MIT).
The returned StreamRequest contains secrets and must never be logged.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .bridge import VacuumBridge
from .xp2p.credentials import derive_tx_video_app_credentials
from .xp2p.worker import StreamRequest

TOKEN_KEYS = ("accessToken", "accesstoken", "token")
IDENTITY_KEYS = {
    "channel_id": ("channelId", "channel_id", "deviceId", "device_id"),
    "product_id": ("productId", "product_id"),
    "device_name": ("deviceName", "device_name"),
    "secret_id": ("secretId", "secret_id"),
    "secret_key": ("secretKey", "secret_key"),
    "app_id": ("appId", "app_id", "appid", "appKey", "app_key", "appkey"),
    "app_secret": ("appSecret", "app_secret", "appsecret", "xp2pSecretKey", "xp2p_secretKey", "xp2p_secret_key"),
}
P2P_KEYS = {
    "p2p_info": ("p2pInfo", "p2p_info", "xp2pInfo", "xp2p_info", "initStringApp", "apiLicense"),
    "app_id": IDENTITY_KEYS["app_id"] + ("xp2pKey", "xp2p_key"),
    "app_secret": IDENTITY_KEYS["app_secret"],
    "secret_id": IDENTITY_KEYS["secret_id"],
    "secret_key": IDENTITY_KEYS["secret_key"],
}
ELIGIBILITY_KEYS = ("isDevUser", "devUser", "isUser")


class ProvisioningError(Exception):
    """Raised when the cloud does not return usable video provisioning."""

    def __init__(self, message: str, report: dict[str, Any]) -> None:
        super().__init__(message)
        self.report = report


def find_text(value: Any, keys: Sequence[str]) -> str | None:
    """Recursive, case-insensitive search for the first non-empty value under any key."""
    wanted = {k.casefold() for k in keys}
    stack = [value]
    while stack:
        item = stack.pop(0)
        if isinstance(item, Mapping):
            for k, v in item.items():
                if str(k).casefold() in wanted and v not in (None, "") and not isinstance(v, Mapping | list):
                    return str(v)
            stack.extend(v for v in item.values() if isinstance(v, Mapping | list))
        elif isinstance(item, list):
            stack.extend(item)
    return None


def _unwrap(response: Any) -> tuple[Any, Any]:
    """Return (data, code); data is the raw response if it is not a success envelope."""
    if isinstance(response, Mapping):
        code = response.get("code")
        if "data" in response and code in (0, None):
            return response["data"], code
        return response, code
    return response, None


def _shape(value: Any, depth: int = 0) -> Any:
    """Describe a response without revealing any values (keys and types only)."""
    if depth > 3:
        return "…"
    if isinstance(value, Mapping):
        return {str(k): _shape(v, depth + 1) for k, v in list(value.items())[:40]}
    if isinstance(value, list):
        return [_shape(value[0], depth + 1)] if value else []
    if value is None:
        return None
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int | float):
        return "number"
    return f"str[{len(str(value))}]"


async def async_provision(bridge: VacuumBridge) -> tuple[StreamRequest, dict[str, Any]]:
    """Run the vendor-app provisioning sequence. Returns the request and a value-free report."""
    ident = bridge.identity()
    did = ident["did"]
    report: dict[str, Any] = {"did_present": bool(did), "uid_present": bool(ident["uid"]), "model": ident["model"]}

    raw, code = _unwrap(await bridge.async_tx_call("access_token", {"os": 1}))
    token = find_text(raw, TOKEN_KEYS)
    report["access_token"] = {"code": code, "token_present": bool(token), "shape": _shape(raw)}

    base: dict[str, Any] = {"did": did, "os": 1}
    if token:
        base["accesstoken"] = token
        base["accessToken"] = token

    identity_params = dict(base)
    if ident["uid"]:
        identity_params["uid"] = ident["uid"]
    if ident["model"]:
        identity_params["model"] = ident["model"]
    identity_raw, code = _unwrap(await bridge.async_tx_call("identity", identity_params))
    fields: dict[str, str] = {}
    for name, keys in IDENTITY_KEYS.items():
        if (value := find_text(identity_raw, keys)) is not None:
            fields[name] = value
    report["identity"] = {"code": code, "fields": sorted(fields), "shape": _shape(identity_raw)}

    p2p_raw, code = _unwrap(await bridge.async_tx_call("p2p", dict(base)))
    for name, keys in P2P_KEYS.items():
        if name not in fields and (value := find_text(p2p_raw, keys)) is not None:
            fields[name] = value
    report["p2p"] = {"code": code, "p2p_info_present": "p2p_info" in fields, "shape": _shape(p2p_raw)}

    if not (fields.get("app_id") and fields.get("app_secret")):
        app_id, app_secret = derive_tx_video_app_credentials(fields.get("secret_id"), fields.get("secret_key"))
        if app_id and "app_id" not in fields:
            fields["app_id"] = app_id
        if app_secret and "app_secret" not in fields:
            fields["app_secret"] = app_secret
        report["app_credentials_derived"] = bool(app_id and app_secret)

    missing = [n for n in ("product_id", "device_name", "p2p_info") if not fields.get(n)]
    report["missing"] = missing
    if missing:
        try:
            elig_raw, code = _unwrap(await bridge.async_tx_call("eligibility", dict(base)))
            report["eligibility"] = {"code": code, "value": find_text(elig_raw, ELIGIBILITY_KEYS), "shape": _shape(elig_raw)}
        except Exception as err:  # noqa: BLE001 - diagnostic only
            report["eligibility"] = {"error": type(err).__name__}
        raise ProvisioningError("Cloud video provisioning is incomplete: missing " + ", ".join(missing), report)

    request = StreamRequest(
        product_id=fields["product_id"],
        device_name=fields["device_name"],
        p2p_info=fields["p2p_info"],
        secret_id=fields.get("secret_id", ""),
        secret_key=fields.get("secret_key", ""),
        channel_id=fields.get("channel_id", ""),
        app_id=fields.get("app_id", ""),
        app_secret=fields.get("app_secret", ""),
        did=did,
    )
    return request, report
