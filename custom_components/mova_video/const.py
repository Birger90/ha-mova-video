"""Constants for mova_video."""

DOMAIN = "mova_video"
DREAME_DOMAIN = "dreame_vacuum"
CONF_DREAME_ENTRY = "dreame_entry_id"

# Dreame/MOVA cloud endpoints for Tencent (TX) video provisioning.
# Source: EvotecIT/homeassistant-dreamelawnmower, dreame_lawn_mower_client/cloud_video.py (MIT).
TX_PATHS = {
    "access_token": "/dreame-third-video/tx/user/accesstoken",
    "identity": "/dreame-third-video/tx/mgr/dev/getIdentity",
    "p2p": "/dreame-third-video/tx/dev/getP2PInfo",
    "eligibility": "/dreame-third-video/tx/dev/isDevUser",
}
