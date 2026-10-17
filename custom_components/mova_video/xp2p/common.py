"""Shared types for the XP2P runtime.

Xp2pHostAssets is adapted from EvotecIT/homeassistant-dreamelawnmower
(dreame_lawn_mower_client/xp2p_host_runtime.py, MIT License).
"""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_PREVIEW_LIMIT = 600
_LOCAL_PATH = re.compile(r"(/[\w.\-]+){2,}")


class VideoRuntimeError(RuntimeError):
    """Raised when the managed XP2P runtime cannot be prepared or used."""


def safe_output_preview(value: Any, sensitive_values: Sequence[str]) -> str:
    """Return a bounded, sanitised one-line preview of worker output."""
    if value is None:
        return ""
    if isinstance(value, bytes | bytearray):
        text = bytes(value).decode("utf-8", "replace")
    else:
        text = str(value)
    text = " ".join(text.split())
    for sensitive in sorted((s for s in sensitive_values if s), key=len, reverse=True):
        text = text.replace(sensitive, "[redacted]")
    text = _LOCAL_PATH.sub("[path]", text)
    if len(text) > _PREVIEW_LIMIT:
        text = text[-_PREVIEW_LIMIT:]
    return text


@dataclass(slots=True, frozen=True)
class Xp2pHostAssets:
    """Verified files used to launch the Bionic XP2P worker on Linux."""

    worker_path: Path
    linker_path: Path
    library_path: Path
    library_search_paths: tuple[Path, ...]
    qemu_path: Path | None = None
    startup_probe: dict[str, Any] | None = field(default=None, compare=False, hash=False)

    def command(self) -> tuple[str, ...]:
        worker = str(self.worker_path)
        linker = str(self.linker_path)
        if self.qemu_path is None:
            return (linker, worker)
        return (
            str(self.qemu_path),
            "-cpu",
            "max",
            "-E",
            f"LD_LIBRARY_PATH={self.library_search_path}",
            linker,
            worker,
        )

    @property
    def library_search_path(self) -> str:
        return os.pathsep.join(str(path) for path in self.library_search_paths)

    def environment(self) -> dict[str, str]:
        environment: dict[str, str] = {}
        if self.qemu_path is None:
            environment["LD_LIBRARY_PATH"] = self.library_search_path
        return environment

    def validate(self) -> None:
        required = {
            "worker": self.worker_path,
            "Bionic linker": self.linker_path,
            "Tencent XP2P library": self.library_path,
        }
        if self.qemu_path is not None:
            required["qemu-aarch64"] = self.qemu_path
        missing = [name for name, path in required.items() if not path.is_file()]
        missing.extend(f"library directory {p}" for p in self.library_search_paths if not p.is_dir())
        for name, path in (("worker", self.worker_path), ("Bionic linker", self.linker_path)):
            if path.is_file() and not os.access(path, os.X_OK):
                missing.append(f"executable {name}")
        if missing:
            raise VideoRuntimeError("XP2P host runtime is incomplete: " + ", ".join(missing) + ".")
