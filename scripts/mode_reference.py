#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WSJTX_APP = REPO_ROOT / "artifacts" / "releases" / "2.7.0" / "wsjtx.app"
DEFAULT_FT2_REF_ROOT = Path("/private/tmp/mode-refs-test/ft2")
DEFAULT_FT2_REF_DECODE = DEFAULT_FT2_REF_ROOT / "ft2-ref-decode"
DEFAULT_FT2_REF_GEN = DEFAULT_FT2_REF_ROOT / "ft2-ref-gen"
DEFAULT_FT2_REF_FRAME = DEFAULT_FT2_REF_ROOT / "ft2-ref-frame"
DEFAULT_FT4_REF_ROOT = Path("/private/tmp/mode-refs-test/ft4")
DEFAULT_FT4_STOCK_DEBUG = DEFAULT_FT4_REF_ROOT / "ft4-stock-debug"


def _resolve_existing(path: Path, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"missing {label}: {resolved}")
    return resolved


def locate_wsjtx_app(explicit: str | None = None) -> Path:
    value = explicit or os.environ.get("WSJTX_APP")
    if value:
        return _resolve_existing(Path(value), "WSJT-X app bundle")
    return _resolve_existing(DEFAULT_WSJTX_APP, "WSJT-X app bundle")


def locate_jt9_binary(explicit_app: str | None = None) -> tuple[Path, Path]:
    app = locate_wsjtx_app(explicit_app)
    binary = app / "Contents" / "MacOS" / "jt9"
    return _resolve_existing(binary, "jt9 binary"), app


def locate_ft2_ref_binary(kind: str, explicit: str | None = None) -> Path:
    env_name = {
        "decode": "FT2_REF_DECODE",
        "gen": "FT2_REF_GEN",
        "frame": "FT2_REF_FRAME",
    }[kind]
    default_path = {
        "decode": DEFAULT_FT2_REF_DECODE,
        "gen": DEFAULT_FT2_REF_GEN,
        "frame": DEFAULT_FT2_REF_FRAME,
    }[kind]
    value = explicit or os.environ.get(env_name)
    if value:
        return _resolve_existing(Path(value), f"FT2 reference {kind} helper")
    return _resolve_existing(default_path, f"FT2 reference {kind} helper")


def locate_ft4_stock_debug(explicit: str | None = None) -> Path:
    value = explicit or os.environ.get("FT4_STOCK_DEBUG")
    if value:
        return _resolve_existing(Path(value), "FT4 stock debug helper")
    return _resolve_existing(DEFAULT_FT4_STOCK_DEBUG, "FT4 stock debug helper")


def jt9_debug_level(profile: str) -> str:
    normalized = profile.strip().lower()
    if normalized == "medium":
        return "2"
    if normalized in {"deep", "deepest"}:
        return "3"
    raise ValueError(f"unsupported stock profile {profile!r}; expected medium or deepest")
