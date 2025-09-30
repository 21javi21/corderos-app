"""Helpers for managing authenticated user sessions."""

from __future__ import annotations

import os
import time
from typing import TypedDict

from fastapi import HTTPException, Request, status


SESSION_DATA_KEY = "corderos_session_user"
_DEFAULT_SESSION_SECONDS = 60 * 60 * 12  # 12 hours


class SessionUser(TypedDict):
    """Shape we persist inside the signed session cookie."""

    uid: str
    is_admin: bool
    issued_at: float
    expires_at: float


def _session_ttl_seconds() -> int:
    raw_value = os.getenv("SESSION_MAX_AGE", str(_DEFAULT_SESSION_SECONDS))
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return _DEFAULT_SESSION_SECONDS
    return max(parsed, 60)  # at least one minute


def establish_session(request: Request, *, uid: str, is_admin: bool) -> SessionUser:
    ttl = _session_ttl_seconds()
    now = time.time()
    session: SessionUser = {
        "uid": uid,
        "is_admin": is_admin,
        "issued_at": now,
        "expires_at": now + ttl,
    }
    request.session[SESSION_DATA_KEY] = session
    return session


def _load_session_user(request: Request) -> SessionUser | None:
    data = request.session.get(SESSION_DATA_KEY)
    if not isinstance(data, dict):  # missing or tampered
        return None
    required_keys = {"uid", "is_admin", "issued_at", "expires_at"}
    if not required_keys.issubset(data.keys()):
        return None
    try:
        return SessionUser(
            uid=str(data["uid"]),
            is_admin=bool(data["is_admin"]),
            issued_at=float(data["issued_at"]),
            expires_at=float(data["expires_at"]),
        )
    except (TypeError, ValueError):
        return None


def clear_session(request: Request) -> None:
    request.session.pop(SESSION_DATA_KEY, None)


def require_user(request: Request) -> SessionUser:
    user = _load_session_user(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Autenticación requerida")

    now = time.time()
    if user["expires_at"] <= now:
        clear_session(request)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="La sesión ha expirado")

    ttl = _session_ttl_seconds()
    # Sliding expiration: refresh when half the time elapsed.
    if user["expires_at"] - now <= ttl / 2:
        refreshed = {
            "uid": user["uid"],
            "is_admin": user["is_admin"],
            "issued_at": now,
            "expires_at": now + ttl,
        }
        request.session[SESSION_DATA_KEY] = refreshed
        return refreshed

    return user


def require_admin(request: Request) -> SessionUser:
    user = require_user(request)
    if not user["is_admin"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Se requieren privilegios de administrador")
    return user


def optional_user(request: Request) -> SessionUser | None:
    try:
        return require_user(request)
    except HTTPException:
        return None
