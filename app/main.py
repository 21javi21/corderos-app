import os
import re
import secrets
import shutil
import time
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path as PathlibPath
from typing import Any

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from fastapi import FastAPI, Depends, HTTPException, Request, Form, File, UploadFile, Path, Query, status
from fastapi.responses import Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

import psycopg2
from psycopg2 import pool, errors
from psycopg2.pool import SimpleConnectionPool

from app import auth_ldap
from app.core.config import settings
from app.security import SessionUser, optional_user, require_user, require_admin
from app.routers import nba as nba_router

# The actual app is defined later in this file
MULTIPLICA_OPCIONES = [1, 2, 3, 4, 5]

AUTO_LOCK_DAYS = 3

HALL_OF_HATE_MAX_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB

# Predefined bet categories for the corderos league
CATEGORIAS_PREDEFINIDAS = [
    "Futbol",
    "Champions League", 
    "La Liga",
    "Premier League",
    "NBA",
    "General",
    "Otros"
]

DEFAULT_HALL_OF_HATE_ENTRIES: list[dict[str, str]] = []

FRAME_CONFIG_PATH = PathlibPath("app/config/hall_of_hate_frames.json")

NBA_TARGET_SEASON_YEAR = 2026
NBA_CONFERENCES = ("West", "East")
NBA_HONOR_CATEGORIES = ("best_record", "mvp", "roy")
NBA_ALL_NBA_SLOT_DEFS: dict[int, dict[str, str]] = {
    1: {"label": "Guard 1", "bucket": "guard"},
    2: {"label": "Guard 2", "bucket": "guard"},
    3: {"label": "Forward 1", "bucket": "forward"},
    4: {"label": "Forward 2", "bucket": "forward"},
    5: {"label": "Forward 3", "bucket": "forward"},
}

NBA_CURRENT_SEASON_ID: int | None = None


def _classify_player_position(raw: str | None) -> str:
    """Return guard/forward bucket from a raw position string."""
    if not raw:
        return "forward"
    normalized = str(raw).strip().upper()
    if not normalized:
        return "forward"
    if normalized.startswith(("PG", "SG", "G")):
        return "guard"
    if "G" in normalized:
        return "guard"
    if normalized.startswith(("SF", "PF", "F", "C")):
        return "forward"
    if "F" in normalized or "C" in normalized:
        return "forward"
    return "forward"

_BASE_FRAME_DEFINITIONS: dict[str, dict[str, str]] = {
    "default": {
        "label": "Default",
        "image_path": "hall_of_hate/frames/frame-default.png",
        "image_box_top": "18%",
        "image_box_left": "9%",
        "image_box_width": "82%",
        "image_box_height": "58%",
        "image_frame_width": "82%",
        "image_frame_height": "58%",
        "image_frame_top": "18%",
        "image_frame_left": "9%",
        "score_top": "82%",
        "score_left": "34%",
        "score_width": "24%",
        "score_height": "24%",
        "score_font_size": "clamp(1.6rem, 3.8vw, 2.6rem)",
        "score_color": "#f7fbff",
        "score_align": "center",
        "name_top": "86%",
        "name_left": "62%",
        "name_width": "48%",
        "name_align": "left",
        "name_color": "#eef6ff",
        "name_font_size": "clamp(0.9rem, 1.6vw, 1.15rem)",
        "name_max_lines": "2",
        "frame_background_color": "rgba(6, 16, 28, 0.92)",
        "frame_border_radius": "24px",
    },
    "devil": {
        "label": "Devil",
        "image_path": "hall_of_hate/frames/frame-devil.png",
        "image_box_top": "14%",
        "image_box_left": "9%",
        "image_box_width": "82%",
        "image_box_height": "54%",
        "image_frame_width": "82%",
        "image_frame_height": "54%",
        "image_frame_top": "14%",
        "image_frame_left": "9%",
        "score_top": "82%",
        "score_left": "42%",
        "score_width": "26%",
        "score_height": "26%",
        "score_font_size": "clamp(1.9rem, 3.8vw, 2.8rem)",
        "score_color": "#f7fbff",
        "score_align": "center",
        "name_top": "85%",
        "name_left": "64%",
        "name_width": "44%",
        "name_align": "left",
        "name_color": "#f5e6c7",
        "name_font_size": "clamp(0.95rem, 1.8vw, 1.25rem)",
        "name_max_lines": "2",
        "frame_background_color": "rgba(6, 16, 28, 0.92)",
        "frame_border_radius": "24px",
    },
}


class FrameStorageError(RuntimeError):
    """Raised when frame metadata cannot be persisted."""


def _load_frame_definitions() -> dict[str, dict[str, str]]:
    def normalize_align(raw: str | None) -> str:
        if not raw:
            return "center"
        normalized = str(raw).strip().lower()
        if normalized in {"flex-start", "flex-end", "center", "space-between", "space-around", "space-evenly"}:
            return normalized
        if normalized == "left":
            return "flex-start"
        if normalized == "right":
            return "flex-end"
        return "center"

    definitions: dict[str, dict[str, str]] = {
        key: dict(value) for key, value in _BASE_FRAME_DEFINITIONS.items()
    }
    try:
        with FRAME_CONFIG_PATH.open("r", encoding="utf-8") as config_file:
            overrides = json.load(config_file)
    except FileNotFoundError:
        return definitions
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"[HallOfHate] No se pudo leer {FRAME_CONFIG_PATH}: {exc}")
        return definitions

    if not isinstance(overrides, dict):
        print("[HallOfHate] El archivo de frames no tiene el formato esperado; usando valores por defecto")
        return definitions

    for key, data in overrides.items():
        if not isinstance(data, dict):
            continue
        merged = dict(definitions.get(key, {}))
        for opt_key, opt_value in data.items():
            merged[opt_key] = str(opt_value)
        merged["score_align"] = normalize_align(merged.get("score_align"))
        definitions[key] = merged
    for merged in definitions.values():
        merged["score_align"] = normalize_align(merged.get("score_align"))
    return definitions


HALL_OF_HATE_FRAMES = _load_frame_definitions()

# Hall of Hate v2 Frame Configuration (Independent from v1)
FRAME_V2_CONFIG_PATH = PathlibPath("app/config/hall_of_hate_frames.json")

def _load_v2_frame_definitions() -> dict[str, dict[str, str]]:
    """Load Hall of Hate v2 frame definitions - completely separate from v1"""
    try:
        with FRAME_V2_CONFIG_PATH.open("r", encoding="utf-8") as config_file:
            v2_frames = json.load(config_file)
    except FileNotFoundError:
        print(f"[HallOfHate v2] Config file {FRAME_V2_CONFIG_PATH} not found, using defaults")
        # Default v2 frame configuration
        v2_frames = {
            "default": {
                "label": "Default Frame",
                "image_path": "hall_of_hate/frames/frame-default.png",
                "background_size": "cover",
                "background_position": "center"
            },
            "devil": {
                "label": "Devil Frame", 
                "image_path": "hall_of_hate/frames/frame-devil.png",
                "background_size": "cover",
                "background_position": "center"
            }
        }
    except Exception as exc:
        print(f"[HallOfHate v2] No se pudo leer {FRAME_V2_CONFIG_PATH}: {exc}")
        return {}

    if not isinstance(v2_frames, dict):
        print("[HallOfHate v2] El archivo de frames v2 no tiene el formato esperado")
        return {}

    return v2_frames

HALL_OF_HATE_V2_FRAMES = _load_v2_frame_definitions()

STATIC_ROOT = PathlibPath("app/images")
HALL_OF_HATE_DIR = STATIC_ROOT / "hall_of_hate"
HALL_OF_HATE_UPLOAD_DIR = PathlibPath(
    os.environ.get("HALL_OF_HATE_UPLOAD_DIR", str(HALL_OF_HATE_DIR / "uploads"))
)
_HALL_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")

FRAME_STORAGE_MODE = "column"
RATINGS_ENABLED = True


def _disable_frame_storage(reason: str) -> None:
    global FRAME_STORAGE_MODE
    if FRAME_STORAGE_MODE != "none":
        print(f"[HallOfHate] Disabling frame metadata support ({reason}); using default frame only.")
    FRAME_STORAGE_MODE = "none"


class ForwardedHeadersMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request, call_next):
        scope = request.scope
        headers = request.headers
        proto = headers.get("x-forwarded-proto")
        if proto:
            scope["scheme"] = proto.split(",")[0].strip()
        host_header = headers.get("x-forwarded-host")
        port_header = headers.get("x-forwarded-port")
        if host_header:
            host_value = host_header.split(",")[0].strip()
            if ":" in host_value:
                host, port_str = host_value.rsplit(":", 1)
                try:
                    port = int(port_str)
                except ValueError:
                    port = None
            else:
                host = host_value
                port = None
            if port is None and port_header:
                port_token = port_header.split(",")[0].strip()
                try:
                    port = int(port_token)
                except ValueError:
                    port = None
            if port is None:
                default_port = scope.get("server", ("", 0))[1]
                if not default_port:
                    default_port = 443 if scope.get("scheme") == "https" else 80
                port = default_port
            scope["server"] = (host, port)
        return await call_next(request)

SESSION_SECRET = os.environ.get("SESSION_SECRET")
if not SESSION_SECRET:
    raise RuntimeError("SESSION_SECRET no está definido")

SESSION_COOKIE_NAME = os.environ.get("SESSION_COOKIE_NAME", "corderos_session")
SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "strict").lower()
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "true").lower() not in {"0", "false", "no"}
try:
    SESSION_MAX_AGE = int(os.environ.get("SESSION_MAX_AGE", str(60 * 60 * 12)))
except (TypeError, ValueError):
    SESSION_MAX_AGE = 60 * 60 * 12

app = FastAPI(title="Corderos App", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ForwardedHeadersMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie=SESSION_COOKIE_NAME,
    same_site=SESSION_COOKIE_SAMESITE,
    https_only=SESSION_COOKIE_SECURE,
    max_age=SESSION_MAX_AGE,
)
templates = Jinja2Templates(directory="app/templates")
app.include_router(auth_ldap.router)
app.include_router(nba_router.router)
app.mount("/static", StaticFiles(directory="app/images"), name="static")

# Root route is defined later as root_redirect for web app functionality

DATABASE_URL = os.environ.get("DATABASE_URL")

pool: SimpleConnectionPool | None = None


def _ensure_schema(conn) -> None:
    global FRAME_STORAGE_MODE
    # Ensure main table exists
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hall_of_hate (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    image_filename TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
                """
            )
        except errors.InsufficientPrivilege:
            conn.rollback()
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'hall_of_hate'
                """
            )
            exists = cur.fetchone() is not None
            if not exists:
                raise
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    # Determine storage for frame metadata
    has_frame_column = False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'hall_of_hate'
              AND column_name = 'frame_key'
            """
        )
        has_frame_column = cur.fetchone() is not None

    global FRAME_STORAGE_MODE
    if has_frame_column:
        FRAME_STORAGE_MODE = "column"
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    ALTER TABLE hall_of_hate
                    ADD COLUMN IF NOT EXISTS frame_key TEXT NOT NULL DEFAULT 'default'
                    """
                )
                cur.execute(
                    "UPDATE hall_of_hate SET frame_key = 'default' WHERE frame_key IS NULL"
                )
                conn.commit()
            except errors.InsufficientPrivilege:
                conn.rollback()
                FRAME_STORAGE_MODE = "table"
            except Exception:
                conn.rollback()
                raise
    else:
        FRAME_STORAGE_MODE = "table"

    if FRAME_STORAGE_MODE == "table":
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS hall_of_hate_frames (
                        entry_id INTEGER PRIMARY KEY REFERENCES hall_of_hate(id) ON DELETE CASCADE,
                        frame_key TEXT NOT NULL DEFAULT 'default'
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO hall_of_hate_frames (entry_id, frame_key)
                    SELECT id, 'default'
                    FROM hall_of_hate
                    ON CONFLICT (entry_id) DO NOTHING
                    """
                )
                conn.commit()
            except errors.InsufficientPrivilege:
                conn.rollback()
                FRAME_STORAGE_MODE = "none"
                print("[HallOfHate] No privileges to manage frame metadata; falling back to default frame only.")
            except Exception:
                conn.rollback()
                raise
    if FRAME_STORAGE_MODE == "none":
        print("[HallOfHate] Frames will not persist; default frame will be used for all entries.")

    # Ensure hall_of_hate table exists (new version with enhanced features)
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hall_of_hate (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    image_filename TEXT NOT NULL,
                    frame_type TEXT DEFAULT 'default',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
                """
            )
        except errors.InsufficientPrivilege:
            conn.rollback()
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'hall_of_hate'
                """
            )
            exists = cur.fetchone() is not None
            if not exists:
                raise HTTPException(status_code=500, detail="Cannot create hall_of_hate table")

        # Ensure hall_of_hate_ratings table exists
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hall_of_hate_ratings (
                    id SERIAL PRIMARY KEY,
                    villain_id INTEGER REFERENCES hall_of_hate(id) ON DELETE CASCADE,
                    user_name TEXT NOT NULL,
                    rating INTEGER CHECK (rating >= 1 AND rating <= 99),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(villain_id, user_name)
                )
                """
            )
        except errors.InsufficientPrivilege:
            conn.rollback()
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'hall_of_hate_ratings'
                """
            )
            exists = cur.fetchone() is not None
            if not exists:
                raise HTTPException(status_code=500, detail="Cannot create hall_of_hate_ratings table")

    # Ensure ratings table and trigger exist
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hall_of_hate_ratings (
                    id SERIAL PRIMARY KEY,
                    entry_id INTEGER NOT NULL REFERENCES hall_of_hate(id) ON DELETE CASCADE,
                    uid TEXT NOT NULL,
                    rating INTEGER NOT NULL CHECK (rating >= 0 AND rating <= 100),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS hall_of_hate_ratings_entry_uid_idx
                ON hall_of_hate_ratings(entry_id, uid)
                """
            )
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION touch_hall_of_hate_rating_updated_at() RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                    $$ LANGUAGE plpgsql
                """
            )
            cur.execute(
                "DROP TRIGGER IF EXISTS hall_of_hate_ratings_touch_updated_at ON hall_of_hate_ratings"
            )
            cur.execute(
                """
                CREATE TRIGGER hall_of_hate_ratings_touch_updated_at
                BEFORE UPDATE ON hall_of_hate_ratings
                FOR EACH ROW
                EXECUTE FUNCTION touch_hall_of_hate_rating_updated_at()
                """
            )
            conn.commit()
        except errors.InsufficientPrivilege:
            conn.rollback()
            print("[HallOfHate] No privileges to manage hall_of_hate_ratings; disabling ratings.")
            global RATINGS_ENABLED
            RATINGS_ENABLED = False
        except Exception:
            conn.rollback()
            raise

    # Ensure NBA picks structures exist
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nba_seasons (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL UNIQUE,
                    label TEXT NOT NULL DEFAULT 'NBA Season',
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nba_teams (
                    id SERIAL PRIMARY KEY,
                    nba_team_id INTEGER UNIQUE,
                    full_name TEXT NOT NULL,
                    abbreviation TEXT NOT NULL,
                    conference TEXT NOT NULL CHECK (conference IN ('East', 'West')),
                    city TEXT,
                    nickname TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS nba_teams_full_name_unique_idx
                ON nba_teams (LOWER(full_name))
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nba_players (
                    id SERIAL PRIMARY KEY,
                    nba_player_id INTEGER UNIQUE,
                    full_name TEXT NOT NULL,
                    team_id INTEGER REFERENCES nba_teams(id) ON DELETE SET NULL,
                    position TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS nba_players_full_name_idx
                ON nba_players (LOWER(full_name))
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nba_playoff_picks (
                    id SERIAL PRIMARY KEY,
                    season_id INTEGER NOT NULL REFERENCES nba_seasons(id) ON DELETE CASCADE,
                    user_uid TEXT NOT NULL,
                    conference TEXT NOT NULL CHECK (conference IN ('East', 'West')),
                    seed INTEGER NOT NULL CHECK (seed BETWEEN 1 AND 8),
                    team_id INTEGER REFERENCES nba_teams(id),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (season_id, user_uid, conference, seed)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nba_honor_picks (
                    id SERIAL PRIMARY KEY,
                    season_id INTEGER NOT NULL REFERENCES nba_seasons(id) ON DELETE CASCADE,
                    user_uid TEXT NOT NULL,
                    category TEXT NOT NULL CHECK (category IN ('best_record', 'mvp', 'roy')),
                    nominee TEXT NOT NULL,
                    nominee_team_id INTEGER REFERENCES nba_teams(id),
                    nominee_team_name TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (season_id, user_uid, category)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nba_all_nba_picks (
                    id SERIAL PRIMARY KEY,
                    season_id INTEGER NOT NULL REFERENCES nba_seasons(id) ON DELETE CASCADE,
                    user_uid TEXT NOT NULL,
                    slot INTEGER NOT NULL CHECK (slot BETWEEN 1 AND 5),
                    player_name TEXT NOT NULL,
                    position TEXT,
                    team_name TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (season_id, user_uid, slot)
                )
                """
            )
            # Relax historical constraints in case table existed with stricter schema
            cur.execute(
                """
                ALTER TABLE nba_all_nba_picks
                DROP CONSTRAINT IF EXISTS nba_all_nba_picks_position_check
                """
            )
            cur.execute(
                """
                ALTER TABLE nba_all_nba_picks
                ALTER COLUMN position DROP NOT NULL
                """
            )
            conn.commit()
        except errors.InsufficientPrivilege:
            conn.rollback()
            print("[NBA] No privileges to create NBA tables; disabling NBA picks feature.")
        except Exception:
            conn.rollback()
            raise


def _ensure_nba_season(conn, *, year: int) -> int | None:
    """Create the NBA season row if missing and return its identifier."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM nba_seasons WHERE year = %s",
                (year,),
            )
            existing = cur.fetchone()
            if existing:
                conn.commit()
                return int(existing[0])
            label = f"NBA {year} Regular Season"
            cur.execute(
                "INSERT INTO nba_seasons (year, label) VALUES (%s, %s) RETURNING id",
                (year, label),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            print(f"[NBA] Created season {year} with id {new_id}")
            return int(new_id)
    except Exception:
        conn.rollback()
        raise


def _parse_locked_value(value: str | None, current: bool) -> bool:
    if value is None:
        return current
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "locked", "bloqueada", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "unlocked", "desbloqueada", "off"}:
        return False
    return current


def _static_path_exists(relative_path: str) -> bool:
    return (STATIC_ROOT / relative_path).exists()


def _resolve_frame_assets() -> dict[str, str | None]:
    assets: dict[str, str | None] = {}
    for key, frame in HALL_OF_HATE_FRAMES.items():
        path = frame.get("image_path")
        if path and _static_path_exists(path):
            assets[key] = path
        else:
            assets[key] = None
    return assets


def _normalize_frame_key(value: str | None) -> str:
    """Return a valid frame key, falling back to ``default`` when needed.

    The database might contain legacy values with extra whitespace or different
    casing, and incoming form data could also present variations. We normalise
    those cases while still preserving any custom frame keys that match after a
    case-insensitive comparison.
    """
    if not value:
        return "default"

    candidate = str(value).strip()
    if candidate in HALL_OF_HATE_FRAMES:
        return candidate

    lowered = candidate.lower()
    for key in HALL_OF_HATE_FRAMES:
        if key.lower() == lowered:
            return key
    return "default"


def _load_nba_teams_by_conference() -> dict[str, list[dict[str, Any]]]:
    teams: dict[str, list[dict[str, Any]]] = {conf: [] for conf in NBA_CONFERENCES}
    if not pool:
        return teams
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, full_name, abbreviation, conference, city, nickname
                FROM nba_teams
                ORDER BY conference, full_name
                """
            )
            for team_id, full_name, abbreviation, conference, city, nickname in cur.fetchall():
                conf = (conference or "").title()
                entry = {
                    "id": team_id,
                    "name": full_name,
                    "abbreviation": abbreviation,
                    "city": city,
                    "nickname": nickname,
                }
                teams.setdefault(conf, []).append(entry)
    except Exception as exc:
        print(f"[NBA] Unable to load teams: {exc}")
    finally:
        pool.putconn(conn)
    return teams


def _load_nba_player_suggestions(limit: int | None = None) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    if not pool:
        return suggestions
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT p.full_name, COALESCE(p.position, ''), COALESCE(t.full_name, '')
                FROM nba_players p
                LEFT JOIN nba_teams t ON t.id = p.team_id
                ORDER BY p.full_name
            """
            params: tuple[Any, ...] = ()
            if limit is not None:
                query += " LIMIT %s"
                params = (limit,)
                cur.execute(query, params)
            else:
                cur.execute(query)
            for full_name, raw_position, team_name in cur.fetchall():
                bucket = _classify_player_position(raw_position)
                suggestions.append(
                    {
                        "name": full_name,
                        "position": (raw_position or "").strip().upper(),
                        "bucket": bucket,
                        "team": team_name,
                    }
                )
    except Exception as exc:
        print(f"[NBA] Unable to load player suggestions: {exc}")
    finally:
        pool.putconn(conn)
    return suggestions


@app.get("/api/nba/players/search")
def nba_player_search(
    q: str = Query("", min_length=1),
    bucket: str | None = Query(None),
    limit: int = Query(25, ge=1, le=50),
):
    term = (q or "").strip()
    if not term:
        return {"items": []}
    normalized_bucket: str | None = None
    if bucket:
        candidate = bucket.lower()
        if candidate in {"guard", "forward"}:
            normalized_bucket = candidate

    if not pool:
        return {"items": []}

    search_cap = limit * 4 if normalized_bucket else limit
    items: list[dict[str, str]] = []
    overflow: list[dict[str, str]] = []
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.full_name, COALESCE(p.position, ''), COALESCE(t.full_name, '')
                FROM nba_players p
                LEFT JOIN nba_teams t ON t.id = p.team_id
                WHERE p.full_name ILIKE %s
                ORDER BY p.full_name
                LIMIT %s
                """,
                (f"%{term}%", search_cap),
            )
            for full_name, raw_position, team_name in cur.fetchall():
                bucket_value = _classify_player_position(raw_position)
                record = {
                    "name": full_name,
                    "position": (raw_position or "").strip().upper(),
                    "team": team_name,
                    "bucket": bucket_value,
                }
                if normalized_bucket and bucket_value != normalized_bucket:
                    if len(overflow) < limit:
                        overflow.append(record)
                    continue
                items.append(record)
                if len(items) >= limit:
                    break
    except Exception as exc:
        print(f"[NBA] player search failed: {exc}")
    finally:
        pool.putconn(conn)
    if normalized_bucket and len(items) < limit:
        remaining = max(0, limit - len(items))
        items.extend(overflow[:remaining])
    return {"items": items}


def _load_user_nba_picks(user_uid: str) -> dict[str, Any]:
    data = {
        "playoff": {conf: {} for conf in NBA_CONFERENCES},
        "honors": {},
        "all_nba": {},
    }
    if not pool or NBA_CURRENT_SEASON_ID is None:
        return data
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.conference,
                       p.seed,
                       p.team_id,
                       t.full_name,
                       t.abbreviation
                FROM nba_playoff_picks p
                LEFT JOIN nba_teams t ON t.id = p.team_id
                WHERE p.season_id = %s
                  AND p.user_uid = %s
                ORDER BY p.conference, p.seed
                """,
                (NBA_CURRENT_SEASON_ID, user_uid),
            )
            for conference, seed, team_id, team_name, abbrev in cur.fetchall():
                conf_key = (conference or "").title()
                data["playoff"].setdefault(conf_key, {})
                data["playoff"][conf_key][int(seed)] = {
                    "team_id": team_id,
                    "team_name": team_name,
                    "abbreviation": abbrev,
                }

            cur.execute(
                """
                SELECT h.category,
                       h.nominee,
                       h.nominee_team_id,
                       h.nominee_team_name,
                       t.full_name
                FROM nba_honor_picks h
                LEFT JOIN nba_teams t ON t.id = h.nominee_team_id
                WHERE h.season_id = %s
                  AND h.user_uid = %s
                """,
                (NBA_CURRENT_SEASON_ID, user_uid),
            )
            for category, nominee, team_id, team_name_override, team_name in cur.fetchall():
                team_label = team_name_override or team_name
                data["honors"][category] = {
                    "nominee": nominee,
                    "team_id": team_id,
                    "team_name": team_label,
                }

            cur.execute(
                """
                SELECT slot, player_name, position, team_name
                FROM nba_all_nba_picks
                WHERE season_id = %s
                  AND user_uid = %s
                ORDER BY slot
                """,
                (NBA_CURRENT_SEASON_ID, user_uid),
            )
            for slot, player_name, position, team_name in cur.fetchall():
                data["all_nba"][int(slot)] = {
                    "player_name": player_name,
                    "position": position,
                    "team_name": team_name,
                }
    except Exception as exc:
        print(f"[NBA] Unable to load picks for {user_uid}: {exc}")
    finally:
        pool.putconn(conn)
    return data


def _replace_user_nba_picks(
    user_uid: str,
    *,
    playoff: dict[str, dict[int, int | None]],
    honors: dict[str, dict[str, Any]],
    all_nba: dict[int, dict[str, str | None]],
) -> None:
    if not pool or NBA_CURRENT_SEASON_ID is None:
        raise HTTPException(status_code=500, detail="NBA picks feature no disponible")

    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM nba_playoff_picks WHERE season_id = %s AND user_uid = %s",
                (NBA_CURRENT_SEASON_ID, user_uid),
            )
            for conference, seeds in playoff.items():
                for seed, team_id in seeds.items():
                    if not team_id:
                        continue
                    cur.execute(
                        """
                        INSERT INTO nba_playoff_picks (season_id, user_uid, conference, seed, team_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (NBA_CURRENT_SEASON_ID, user_uid, conference, seed, team_id),
                    )

            cur.execute(
                "DELETE FROM nba_honor_picks WHERE season_id = %s AND user_uid = %s",
                (NBA_CURRENT_SEASON_ID, user_uid),
            )
            for category, payload in honors.items():
                nominee = (payload.get("nominee") or "").strip()
                team_id = payload.get("team_id")
                team_name = payload.get("team_name")
                if not nominee:
                    continue
                cur.execute(
                    """
                    INSERT INTO nba_honor_picks (season_id, user_uid, category, nominee, nominee_team_id, nominee_team_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (NBA_CURRENT_SEASON_ID, user_uid, category, nominee, team_id, team_name),
                )

            cur.execute(
                "DELETE FROM nba_all_nba_picks WHERE season_id = %s AND user_uid = %s",
                (NBA_CURRENT_SEASON_ID, user_uid),
            )
            for slot, payload in all_nba.items():
                player_name = (payload.get("player_name") or "").strip()
                team_name = payload.get("team_name")
                position = (payload.get("position") or None)
                if not player_name:
                    continue
                cur.execute(
                    """
                    INSERT INTO nba_all_nba_picks (season_id, user_uid, slot, player_name, position, team_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (NBA_CURRENT_SEASON_ID, user_uid, slot, player_name, position, team_name),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _merge_form_into_picks(
    picks: dict[str, Any],
    *,
    playoff_payload: dict[str, dict[int, int | None]],
    honors_payload: dict[str, dict[str, Any]],
    all_nba_payload: dict[int, dict[str, str | None]],
    teams_by_id: dict[str, dict[str, Any]],
    player_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    merged = {
        "playoff": {conf: dict(picks.get("playoff", {}).get(conf, {})) for conf in NBA_CONFERENCES},
        "honors": dict(picks.get("honors", {})),
        "all_nba": dict(picks.get("all_nba", {})),
    }
    for conference, seeds in playoff_payload.items():
        for seed, team_id in seeds.items():
            if not team_id:
                merged["playoff"].setdefault(conference, {}).pop(seed, None)
                continue
            team = teams_by_id.get(str(team_id)) or teams_by_id.get(team_id) or {}
            merged["playoff"].setdefault(conference, {})[seed] = {
                "team_id": team_id,
                "team_name": team.get("name"),
                "abbreviation": team.get("abbreviation"),
            }

    for category, payload in honors_payload.items():
        nominee = payload.get("nominee")
        if not nominee:
            merged["honors"].pop(category, None)
            continue
        team_name = payload.get("team_name")
        if not team_name:
            lookup = player_lookup.get(nominee.strip().lower()) if nominee else None
            if lookup and lookup.get("team"):
                team_name = lookup["team"]
        merged["honors"][category] = {
            "nominee": nominee,
            "team_name": team_name,
        }

    for slot, payload in all_nba_payload.items():
        player_name = payload.get("player_name")
        if not player_name:
            merged["all_nba"].pop(slot, None)
            continue
        key = str(player_name).strip().lower()
        lookup = player_lookup.get(key) or {}
        merged["all_nba"][slot] = {
            "player_name": player_name,
            "team_name": payload.get("team_name") or lookup.get("team"),
            "position": payload.get("position") or lookup.get("bucket"),
        }

    return merged


def _load_all_users_nba_picks() -> list[dict[str, Any]]:
    if not pool or NBA_CURRENT_SEASON_ID is None:
        return []
    conn = pool.getconn()
    try:
        users: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.user_uid,
                       p.conference,
                       p.seed,
                       t.full_name,
                       t.abbreviation
                FROM nba_playoff_picks p
                LEFT JOIN nba_teams t ON t.id = p.team_id
                WHERE p.season_id = %s
                ORDER BY p.user_uid, p.conference, p.seed
                """,
                (NBA_CURRENT_SEASON_ID,),
            )
            for uid, conference, seed, team_name, abbreviation in cur.fetchall():
                record = users.setdefault(
                    uid,
                    {
                        "user_uid": uid,
                        "playoff": {conf: {} for conf in NBA_CONFERENCES},
                        "honors": {},
                        "all_nba": {},
                    },
                )
                conf_key = (conference or "").title()
                record["playoff"].setdefault(conf_key, {})
                record["playoff"][conf_key][int(seed)] = {
                    "team_name": team_name,
                    "abbreviation": abbreviation,
                }

            cur.execute(
                """
                SELECT user_uid, category, nominee, nominee_team_name
                FROM nba_honor_picks
                WHERE season_id = %s
                ORDER BY user_uid, category
                """,
                (NBA_CURRENT_SEASON_ID,),
            )
            for uid, category, nominee, team_name in cur.fetchall():
                record = users.setdefault(
                    uid,
                    {
                        "user_uid": uid,
                        "playoff": {conf: {} for conf in NBA_CONFERENCES},
                        "honors": {},
                        "all_nba": {},
                    },
                )
                record["honors"][category] = {
                    "nominee": nominee,
                    "team_name": team_name,
                }

            cur.execute(
                """
                SELECT user_uid, slot, player_name, position, team_name
                FROM nba_all_nba_picks
                WHERE season_id = %s
                ORDER BY user_uid, slot
                """,
                (NBA_CURRENT_SEASON_ID,),
            )
            for uid, slot, player_name, position, team_name in cur.fetchall():
                record = users.setdefault(
                    uid,
                    {
                        "user_uid": uid,
                        "playoff": {conf: {} for conf in NBA_CONFERENCES},
                        "honors": {},
                        "all_nba": {},
                    },
                )
                record["all_nba"][int(slot)] = {
                    "player_name": player_name,
                    "position": position,
                    "team_name": team_name,
                }
        ordered = sorted(users.values(), key=lambda item: item["user_uid"])
        return ordered
    except Exception as exc:
        print(f"[NBA] Unable to load aggregated picks: {exc}")
        return []
    finally:
        pool.putconn(conn)

def _store_frame_key(cur, entry_id: int, frame_key: str) -> None:
    key = _normalize_frame_key(frame_key)
    global FRAME_STORAGE_MODE
    if FRAME_STORAGE_MODE == "column":
        try:
            cur.execute(
                "UPDATE hall_of_hate SET frame_key = %s WHERE id = %s",
                (key, entry_id),
            )
        except errors.InsufficientPrivilege as exc:
            _disable_frame_storage("no privilege to update hall_of_hate.frame_key column")
            raise FrameStorageError("No privileges to update hall_of_hate.frame_key") from exc
        except errors.UndefinedColumn as exc:
            _disable_frame_storage("hall_of_hate.frame_key column missing")
            raise FrameStorageError("hall_of_hate.frame_key column missing") from exc
        except Exception:
            raise
        return
    if FRAME_STORAGE_MODE == "table":
        try:
            cur.execute(
                """
                INSERT INTO hall_of_hate_frames (entry_id, frame_key)
                VALUES (%s, %s)
                ON CONFLICT (entry_id) DO UPDATE SET frame_key = EXCLUDED.frame_key
                """,
                (entry_id, key),
            )
        except errors.InsufficientPrivilege as exc:
            _disable_frame_storage("no privileges to maintain hall_of_hate_frames")
            raise FrameStorageError("No privileges to maintain hall_of_hate_frames") from exc
        except errors.UndefinedTable as exc:
            _disable_frame_storage("hall_of_hate_frames table missing")
            raise FrameStorageError("hall_of_hate_frames table missing") from exc
        except Exception:
            raise
    # If FRAME_STORAGE_MODE == "none", do nothing (default frame only)


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _has_result_fields(winners: tuple[str | None, ...], losers: tuple[str | None, ...]) -> bool:
    return any(_empty_to_none(item) for item in winners) and any(_empty_to_none(item) for item in losers)


def _compute_auto_locked(
    manual_locked: bool,
    result_recorded: date | None,
    auto_lock_released: bool,
    winners: tuple[str | None, ...],
    losers: tuple[str | None, ...],
) -> tuple[bool, bool]:
    has_result = _has_result_fields(winners, losers)
    auto_locked = False
    if has_result and result_recorded and not auto_lock_released:
        if date.today() >= result_recorded + timedelta(days=AUTO_LOCK_DAYS):
            auto_locked = True
    effective_locked = manual_locked or auto_locked
    return auto_locked, effective_locked


def _compute_estado_label(winners: tuple[str | None, ...], losers: tuple[str | None, ...]) -> str:
    return "CERRADA" if _has_result_fields(winners, losers) else "ACTIVA"


def _slugify(name: str) -> str:
    return _HALL_SLUG_PATTERN.sub("_", name.lower()).strip("_")


def _resolve_default_image_filename(filename: str | None) -> str | None:
    if not filename:
        return None
    expected_path = HALL_OF_HATE_DIR / filename
    if expected_path.exists():
        return filename
    target_slug = _slugify(PathlibPath(filename).stem)
    for image_path in HALL_OF_HATE_DIR.glob("*.png"):
        if _slugify(image_path.stem) == target_slug:
            return image_path.name
    return None


def _seed_hall_of_hate_defaults(conn) -> None:
    attempts = 0
    while True:
        attempts += 1
        try:
            with conn.cursor() as cur:
                for entry in DEFAULT_HALL_OF_HATE_ENTRIES:
                    name = entry["name"]
                    frame_key = _normalize_frame_key(entry.get("frame_key"))
                    cur.execute(
                        "SELECT id, image_filename FROM hall_of_hate WHERE lower(name) = lower(%s)",
                        (name,),
                    )
                    row = cur.fetchone()
                    resolved_image = _resolve_default_image_filename(entry.get("image_filename"))
                    if row:
                        entry_id, current_image = row
                        if not current_image and resolved_image:
                            cur.execute(
                                "UPDATE hall_of_hate SET image_filename = %s WHERE id = %s",
                                (resolved_image, entry_id),
                            )
                        _store_frame_key(cur, entry_id, frame_key)
                        continue
                    if FRAME_STORAGE_MODE == "column":
                        cur.execute(
                            "INSERT INTO hall_of_hate (name, image_filename, frame_key) VALUES (%s, %s, %s) RETURNING id",
                            (name, resolved_image, frame_key),
                        )
                        entry_id = cur.fetchone()[0]
                        _store_frame_key(cur, entry_id, frame_key)
                    else:
                        cur.execute(
                            "INSERT INTO hall_of_hate (name, image_filename) VALUES (%s, %s) RETURNING id",
                            (name, resolved_image),
                        )
                        entry_id = cur.fetchone()[0]
                        _store_frame_key(cur, entry_id, frame_key)
                if FRAME_STORAGE_MODE == "column":
                    cur.execute(
                        """
                        UPDATE hall_of_hate
                        SET frame_key = 'default'
                        WHERE frame_key IS NULL
                           OR NOT (frame_key = ANY(%s))
                        """,
                        (list(HALL_OF_HATE_FRAMES.keys()),),
                    )
            conn.commit()
            return
        except FrameStorageError:
            conn.rollback()
            if attempts >= 2:
                return
            continue
        except Exception:
            conn.rollback()
            raise


def _fetch_hall_of_hate_db_entries(current_uid: str | None) -> list[dict[str, str | None | float | int]]:
    if not pool:
        return []
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            if FRAME_STORAGE_MODE == "column":
                frame_expr = "COALESCE(h.frame_key, 'default')"
                join_clause = ""
                group_columns = "h.id, h.name, h.image_filename, h.frame_key, h.created_at"
            elif FRAME_STORAGE_MODE == "table":
                frame_expr = "COALESCE(f.frame_key, 'default')"
                join_clause = "LEFT JOIN hall_of_hate_frames f ON f.entry_id = h.id"
                group_columns = "h.id, h.name, h.image_filename, f.frame_key, h.created_at"
            else:  # none
                frame_expr = "'default'"
                join_clause = ""
                group_columns = "h.id, h.name, h.image_filename, h.created_at"

            if RATINGS_ENABLED:
                ratings_join = "LEFT JOIN hall_of_hate_ratings r ON r.entry_id = h.id"
                select_ratings = "COALESCE(AVG(r.rating), 99) AS avg_rating, COUNT(r.rating) AS rating_count, COALESCE(MAX(CASE WHEN r.uid = %s THEN r.rating END), 99) AS current_user_rating"
                group_by = group_columns
                params = (current_uid,)
            else:
                ratings_join = ""
                select_ratings = "99 AS avg_rating, 0 AS rating_count, 99 AS current_user_rating"
                group_by = group_columns
                params = tuple()

            query = f"""
                SELECT
                    h.id,
                    h.name,
                    h.image_filename,
                    {frame_expr} AS frame_key,
                    {select_ratings}
                FROM hall_of_hate h
                {join_clause}
                {ratings_join}
                GROUP BY {group_by}
                ORDER BY h.created_at DESC, h.id DESC
            """
            try:
                cur.execute(query, params)
            except errors.UndefinedTable:
                _disable_frame_storage("hall_of_hate_frames table missing")
                if FRAME_STORAGE_MODE != "table":
                    return _fetch_hall_of_hate_db_entries(current_uid)
                raise
            rows = cur.fetchall()
    finally:
        pool.putconn(conn)

    entries: list[dict[str, str | None | float | int]] = []
    for entry_id, name, image_filename, frame_key, avg_rating, rating_count, user_rating in rows:
        image_path = None
        if image_filename:
            candidate = f"hall_of_hate/{image_filename}"
            if _static_path_exists(candidate):
                image_path = candidate
        frame_key = _normalize_frame_key(frame_key)
        
        # Use proper average calculation that considers all LDAP users default to 99
        proper_avg_value = _calculate_proper_average_hate(entry_id)
        count_value = int(rating_count or 0)
        user_rating_value = int(user_rating) if user_rating is not None else 99
        entries.append({
            "id": entry_id,
            "name": name,
            "image": image_path,
            "frame_key": frame_key,
            "average_hate": proper_avg_value,
            "ratings_count": count_value,
            "user_rating": user_rating_value,
        })
    return entries


def _insert_hall_of_hate_entry(name: str, image_filename: str, frame_key: str) -> int:
    if not pool:
        raise HTTPException(status_code=500, detail="Conexión a base de datos no inicializada")
    attempts = 0
    while True:
        attempts += 1
        conn = pool.getconn()
        try:
            with conn, conn.cursor() as cur:
                if FRAME_STORAGE_MODE == "column":
                    cur.execute(
                        """
                        INSERT INTO hall_of_hate (name, image_filename, frame_key)
                        VALUES (%s, %s, %s)
                        RETURNING id
                        """,
                        (name, image_filename, _normalize_frame_key(frame_key)),
                    )
                    entry_id = cur.fetchone()[0]
                    _store_frame_key(cur, entry_id, frame_key)
                elif FRAME_STORAGE_MODE == "table":
                    cur.execute(
                        """
                        INSERT INTO hall_of_hate (name, image_filename)
                        VALUES (%s, %s)
                        RETURNING id
                        """,
                        (name, image_filename),
                    )
                    entry_id = cur.fetchone()[0]
                    _store_frame_key(cur, entry_id, frame_key)
                else:  # FRAME_STORAGE_MODE == "none"
                    cur.execute(
                        """
                        INSERT INTO hall_of_hate (name, image_filename)
                        VALUES (%s, %s)
                        RETURNING id
                        """,
                        (name, image_filename),
                    )
                    entry_id = cur.fetchone()[0]
            return entry_id
        except FrameStorageError:
            if attempts >= 2:
                raise
            # retry with updated FRAME_STORAGE_MODE (likely "none")
        finally:
            pool.putconn(conn)


def _save_hall_of_hate_image(upload: UploadFile, display_name: str) -> str:
    content_type = (upload.content_type or "").lower()
    if content_type != "image/png":
        raise HTTPException(status_code=400, detail="Solo se permiten imágenes PNG")

    try:
        data = upload.file.read(HALL_OF_HATE_MAX_UPLOAD_BYTES + 1)
    finally:
        upload.file.close()

    if not data:
        raise HTTPException(status_code=400, detail="El archivo está vacío")
    if len(data) > HALL_OF_HATE_MAX_UPLOAD_BYTES:
        max_mb = HALL_OF_HATE_MAX_UPLOAD_BYTES // (1024 * 1024)
        raise HTTPException(status_code=413, detail=f"La imagen supera el tamaño máximo de {max_mb} MB")
    if not data.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=400, detail="El archivo no es un PNG válido")

    HALL_OF_HATE_DIR.mkdir(parents=True, exist_ok=True)
    HALL_OF_HATE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    base_slug = _slugify(display_name) or f"entry_{secrets.token_hex(2)}"
    filename = f"{base_slug}_{secrets.token_hex(4)}.png"
    destination = HALL_OF_HATE_UPLOAD_DIR / filename
    with destination.open("wb") as out_file:
        out_file.write(data)
    try:
        relative_prefix = HALL_OF_HATE_UPLOAD_DIR.relative_to(HALL_OF_HATE_DIR)
    except ValueError:
        return filename
    if relative_prefix == PathlibPath("."):
        return filename
    return str(relative_prefix / filename)


def _get_hall_of_hate_entry(entry_id: int) -> dict[str, str | int | None] | None:
    if not pool:
        return None
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            if FRAME_STORAGE_MODE == "column":
                cur.execute(
                    """
                    SELECT id, name, image_filename, frame_key
                    FROM hall_of_hate
                    WHERE id = %s
                    """,
                    (entry_id,),
                )
            elif FRAME_STORAGE_MODE == "table":
                try:
                    cur.execute(
                        """
                        SELECT
                            h.id,
                            h.name,
                            h.image_filename,
                            COALESCE(f.frame_key, 'default')
                        FROM hall_of_hate h
                        LEFT JOIN hall_of_hate_frames f ON f.entry_id = h.id
                        WHERE h.id = %s
                        """,
                        (entry_id,),
                    )
                except errors.UndefinedTable:
                    _disable_frame_storage("hall_of_hate_frames table missing")
                    if FRAME_STORAGE_MODE != "table":
                        return _get_hall_of_hate_entry(entry_id)
                    raise
            else:
                cur.execute(
                    """
                    SELECT id, name, image_filename, 'default' AS frame_key
                    FROM hall_of_hate
                    WHERE id = %s
                    """,
                    (entry_id,),
                )
            row = cur.fetchone()
    finally:
        pool.putconn(conn)
    if not row:
        return None
    frame_key = _normalize_frame_key(row[3])
    image_filename: str | None = row[2]
    image_path: str | None = None
    if image_filename:
        candidate = f"hall_of_hate/{image_filename}"
        image_path = candidate if _static_path_exists(candidate) else None

    return {
        "id": row[0],
        "name": row[1],
        "image_filename": image_filename,
        "image_path": image_path,
        "frame_key": frame_key,
    }


def _update_hall_of_hate_entry(entry_id: int, name: str, image_filename: str | None, frame_key: str) -> None:
    if not pool:
        raise HTTPException(status_code=500, detail="Conexión a base de datos no inicializada")
    attempts = 0
    while True:
        attempts += 1
        conn = pool.getconn()
        try:
            with conn, conn.cursor() as cur:
                if image_filename is None:
                    cur.execute(
                        """
                        UPDATE hall_of_hate
                        SET name = %s
                        WHERE id = %s
                        """,
                        (name, entry_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE hall_of_hate
                        SET name = %s,
                            image_filename = %s
                        WHERE id = %s
                        """,
                        (name, image_filename, entry_id),
                    )
                _store_frame_key(cur, entry_id, frame_key)
            return
        except FrameStorageError:
            if attempts >= 2:
                raise
        finally:
            pool.putconn(conn)


def _delete_hall_of_hate_entry(entry_id: int) -> str | None:
    if not pool:
        raise HTTPException(status_code=500, detail="Conexión a base de datos no inicializada")
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT image_filename FROM hall_of_hate WHERE id = %s",
                (entry_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            image_filename: str | None = row[0]
            cur.execute("DELETE FROM hall_of_hate WHERE id = %s", (entry_id,))
    finally:
        pool.putconn(conn)
    return image_filename


def _get_hall_of_hate_rating(entry_id: int, uid: str) -> int | None:
    if not pool or not RATINGS_ENABLED:
        return None
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rating
                FROM hall_of_hate_ratings
                WHERE entry_id = %s AND uid = %s
                """,
                (entry_id, uid),
            )
            row = cur.fetchone()
    finally:
        pool.putconn(conn)
    if not row:
        return None
    return int(row[0])


def _set_hall_of_hate_rating(entry_id: int, uid: str, rating: int) -> None:
    if not pool:
        raise HTTPException(status_code=500, detail="Conexión a base de datos no inicializada")
    if not RATINGS_ENABLED:
        raise HTTPException(status_code=503, detail="El registro de odio no está disponible en este momento")
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hall_of_hate_ratings (entry_id, uid, rating)
                VALUES (%s, %s, %s)
                ON CONFLICT (entry_id, uid)
                DO UPDATE SET rating = EXCLUDED.rating
                """,
                (entry_id, uid, rating),
            )
    finally:
        pool.putconn(conn)


def _calculate_proper_average_hate(entry_id: int) -> float:
    """Calculate average hate considering all LDAP users default to 99"""
    if not pool or not RATINGS_ENABLED:
        print(f"[DEBUG] Ratings disabled or no pool for entry {entry_id}")
        return 99.0
    
    # Get all LDAP users
    all_users = auth_ldap.fetch_all_user_uids()
    if not all_users:
        print(f"[DEBUG] No LDAP users found for entry {entry_id}")
        return 99.0
    
    total_users = len(all_users)
    print(f"[DEBUG] Total LDAP users: {total_users}, users: {all_users}")
    
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            # Get all current ratings for this entry
            cur.execute(
                """
                SELECT uid, rating 
                FROM hall_of_hate_ratings 
                WHERE entry_id = %s
                """,
                (entry_id,)
            )
            ratings = cur.fetchall()
            print(f"[DEBUG] Found ratings for entry {entry_id}: {ratings}")
            
            # Create a map of user ratings
            user_ratings = {uid: rating for uid, rating in ratings}
            
            # Calculate average with all users defaulting to 99
            total_score = 0
            user_scores = []
            for user in all_users:
                score = user_ratings.get(user, 99)  # Default to 99 if not rated
                total_score += score
                user_scores.append(f"{user}:{score}")
                
            average = total_score / total_users
            print(f"[DEBUG] Entry {entry_id} calculation: {' + '.join(user_scores)} = {total_score} / {total_users} = {average}")
            return average
            
    finally:
        pool.putconn(conn)


def _hall_of_hate_entries(current_user: SessionUser | None) -> list[dict[str, str | None | float | int]]:
    uid = current_user["uid"] if current_user else None
    return _fetch_hall_of_hate_db_entries(uid)

def _get_hall_of_hate_entries(current_user: SessionUser | None) -> list[dict[str, str | None | float | int]]:
    """Get Hall of Hate v2 entries with calculated averages"""
    global pool
    if not pool:
        return []
    
    # Clean up orphaned ratings before calculating averages (temporarily disabled for troubleshooting)
    # try:
    #     _cleanup_orphaned_ratings()
    # except Exception as e:
    #     print(f"Warning: Could not clean up orphaned ratings: {e}")
    
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                hv2.id,
                hv2.name, 
                hv2.image_filename,
                hv2.frame_type,
                COALESCE(
                    (SELECT AVG(rating) FROM hall_of_hate_v2_ratings WHERE villain_id = hv2.id), 
                    99
                ) as average_hate
            FROM hall_of_hate_v2 hv2
            ORDER BY average_hate DESC
        """)
        results = cursor.fetchall()
        
        entries = []
        for row in results:
            villain_id, name, image_filename, frame_type, average_hate = row
            
            # Get user's rating if available
            user_rating = 99  # Default for unrated
            if current_user:
                # Handle both test users (with "id") and authenticated users (with "uid")
                user_id = current_user.get("id") or current_user.get("uid")
                if user_id:
                    cursor.execute(
                        "SELECT rating FROM hall_of_hate_v2_ratings WHERE villain_id = %s AND user_name = %s",
                        (villain_id, user_id)
                    )
                    rating_result = cursor.fetchone()
                    if rating_result:
                        user_rating = rating_result[0]
            
            entries.append({
                "id": villain_id,
                "name": name,
                "hate_score": int(average_hate),
                "image_filename": image_filename,
                "frame_type": frame_type or "default",
                "user_rating": user_rating
            })
        
        return entries
    except Exception as e:
        print(f"Error fetching v2 entries: {e}")
        return []
    finally:
        pool.putconn(conn)

@app.on_event("startup")
def startup_db():
    global pool
    if not DATABASE_URL:
        # Usa el ConfigMap ya desplegado en K8s; localmente puedes exportar DATABASE_URL
        raise RuntimeError("DATABASE_URL no está definido")
    pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
    HALL_OF_HATE_DIR.mkdir(parents=True, exist_ok=True)
    HALL_OF_HATE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    conn = pool.getconn()
    try:
        _ensure_schema(conn)
        _seed_hall_of_hate_defaults(conn)
        global NBA_CURRENT_SEASON_ID
        NBA_CURRENT_SEASON_ID = _ensure_nba_season(conn, year=NBA_TARGET_SEASON_YEAR)
        if NBA_CURRENT_SEASON_ID is None:
            print("[NBA] Warning: could not initialize NBA season record.")
    finally:
        pool.putconn(conn)

@app.on_event("shutdown")
def shutdown_db():
    global pool
    if pool:
        pool.closeall()
        pool = None

@app.get("/", response_class=HTMLResponse)
def root_redirect(current_user: SessionUser | None = Depends(optional_user)):
    # redirect logged users to their dashboard, others to login
    if current_user:
        target = "/auth/dashboard_admin" if current_user["is_admin"] else "/auth/dashboard_user"
        return RedirectResponse(url=target)
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, current_user: SessionUser | None = Depends(optional_user)):
    if current_user:
        target = "/auth/dashboard_admin" if current_user["is_admin"] else "/auth/dashboard_user"
        return RedirectResponse(url=target)
    # render the login form
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/user_dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("user_dashboard.html", {"request": request})

@app.get("/test-download", response_class=HTMLResponse)
def test_download_page(request: Request):
    """Test page for debugging card download functionality without authentication"""
    return templates.TemplateResponse("test_download.html", {"request": request})


@app.get("/nba-playoffs", response_class=HTMLResponse)
def nba_playoffs_page(request: Request, current_user: SessionUser = Depends(require_user)):
    teams = _load_nba_teams_by_conference()
    picks = _load_user_nba_picks(current_user["uid"])
    player_suggestions = _load_nba_player_suggestions()
    guard_suggestions = [item for item in player_suggestions if item.get("bucket") == "guard"]
    forward_suggestions = [item for item in player_suggestions if item.get("bucket") != "guard"]
    player_lookup = {
        item["name"].lower(): {
            "team": item.get("team"),
            "position": item.get("position"),
            "bucket": item.get("bucket"),
        }
        for item in player_suggestions
    }
    slot_entries = [
        {"slot": slot, "label": data["label"], "bucket": data["bucket"]}
        for slot, data in NBA_ALL_NBA_SLOT_DEFS.items()
    ]
    saved = request.query_params.get("saved")
    status_message = "✅ Selecciones guardadas" if saved else None
    context = {
        "request": request,
        "season_year": NBA_TARGET_SEASON_YEAR,
        "teams": teams,
        "picks": picks,
        "player_suggestions": player_suggestions,
        "guard_suggestions": guard_suggestions,
        "forward_suggestions": forward_suggestions,
        "player_lookup": player_lookup,
        "slot_entries": slot_entries,
        "honor_categories": NBA_HONOR_CATEGORIES,
        "status_message": status_message,
        "error_message": None,
    }
    return templates.TemplateResponse(
        "nba_playoffs.html",
        context,
    )


@app.post("/nba-playoffs", response_class=HTMLResponse)
async def nba_playoffs_submit(request: Request, current_user: SessionUser = Depends(require_user)):
    form = await request.form()
    teams = _load_nba_teams_by_conference()
    player_suggestions = _load_nba_player_suggestions()
    guard_suggestions = [item for item in player_suggestions if item.get("bucket") == "guard"]
    forward_suggestions = [item for item in player_suggestions if item.get("bucket") != "guard"]
    player_lookup = {
        item["name"].lower(): {
            "team": item.get("team"),
            "position": item.get("position"),
            "bucket": item.get("bucket"),
        }
        for item in player_suggestions
    }
    slot_entries = [
        {"slot": slot, "label": data["label"], "bucket": data["bucket"]}
        for slot, data in NBA_ALL_NBA_SLOT_DEFS.items()
    ]
    teams_by_id_int = {team["id"]: team for bucket in teams.values() for team in bucket}
    teams_by_id = {str(team_id): team for team_id, team in teams_by_id_int.items()}
    teams_for_merge: dict[Any, dict[str, Any]] = {}
    teams_for_merge.update(teams_by_id_int)
    teams_for_merge.update(teams_by_id)

    playoff_payload: dict[str, dict[int, int | None]] = {conf: {} for conf in NBA_CONFERENCES}
    duplicates: list[str] = []
    invalid_team_errors: list[str] = []
    for conference in NBA_CONFERENCES:
        seen: set[int] = set()
        for seed in range(1, 9):
            field = f"{conference.lower()}_seed_{seed}"
            raw_value = form.get(field)
            team_id: int | None = None
            if raw_value:
                team_entry = teams_by_id.get(raw_value)
                if not team_entry:
                    invalid_team_errors.append(f"{conference} #{seed}")
                    playoff_payload[conference][seed] = None
                    continue
                team_id = int(team_entry["id"])
                if team_id in seen:
                    duplicates.append(f"{conference} #{seed}")
                else:
                    seen.add(team_id)
            playoff_payload[conference][seed] = team_id

    honors_payload: dict[str, dict[str, Any]] = {}
    for category in NBA_HONOR_CATEGORIES:
        if category == "best_record":
            team_field = form.get("honor_best_record_team_id")
            team_info = teams_by_id.get(team_field) if team_field else None
            honors_payload[category] = {
                "nominee": (team_info["name"] if team_info else ""),
                "team_id": int(team_info["id"]) if team_info else None,
                "team_name": team_info["name"] if team_info else None,
            }
        else:
            nominee = (form.get(f"honor_{category}_name") or "").strip()
            team_name = (form.get(f"honor_{category}_team") or "").strip() or None
            if nominee and not team_name:
                lookup = player_lookup.get(nominee.lower())
                if lookup and lookup.get("team"):
                    team_name = lookup["team"]
            honors_payload[category] = {
                "nominee": nominee,
                "team_id": None,
                "team_name": team_name,
            }

    all_nba_payload: dict[int, dict[str, str | None]] = {}
    guard_count = 0
    forward_count = 0
    bucket_mismatches: list[int] = []
    for slot_entry in slot_entries:
        slot = slot_entry["slot"]
        required_bucket = slot_entry["bucket"]
        player = (form.get(f"all_nba_slot_{slot}_player") or "").strip()
        team_name = (form.get(f"all_nba_slot_{slot}_team") or "").strip() or None
        all_nba_payload[slot] = {
            "player_name": player,
            "team_name": team_name,
            "position": required_bucket,
        }
        if player:
            lookup_entry = player_lookup.get(player.lower())
            bucket = _classify_player_position(lookup_entry.get("position") if lookup_entry else None)
            if not team_name and lookup_entry and lookup_entry.get("team"):
                team_name = lookup_entry["team"]
                all_nba_payload[slot]["team_name"] = team_name
            if bucket == "guard":
                guard_count += 1
            else:
                forward_count += 1
            if bucket != required_bucket:
                all_nba_payload[slot]["position"] = bucket
                bucket_mismatches.append(slot)
        else:
            all_nba_payload[slot]["position"] = required_bucket

    error_messages: list[str] = []
    if invalid_team_errors:
        error_messages.append("Equipo inválido seleccionado en: " + ", ".join(invalid_team_errors))
    if duplicates:
        error_messages.append("Cada equipo solo puede elegirse una vez por conferencia. Revisa: " + ", ".join(duplicates))
    if bucket_mismatches:
        label_lookup = {entry["slot"]: entry["label"] for entry in slot_entries}
        labels = [label_lookup.get(slot, f"Slot {slot}") for slot in bucket_mismatches]
        error_messages.append("Revisa las posiciones: los slots tienen un rol fijo (Guard/Forward). Afectados: " + ", ".join(labels))
    if guard_count > 2 or forward_count > 3:
        error_messages.append("Debes elegir máximo 2 guards y máximo 3 forwards.")

    if error_messages:
        base_picks = _load_user_nba_picks(current_user["uid"])
        attempt_picks = _merge_form_into_picks(
            base_picks,
            playoff_payload=playoff_payload,
            honors_payload=honors_payload,
            all_nba_payload=all_nba_payload,
            teams_by_id=teams_for_merge,
            player_lookup=player_lookup,
        )
        return templates.TemplateResponse(
            "nba_playoffs.html",
            {
                "request": request,
                "season_year": NBA_TARGET_SEASON_YEAR,
                "teams": teams,
                "picks": attempt_picks,
                "player_suggestions": player_suggestions,
                "guard_suggestions": guard_suggestions,
                "forward_suggestions": forward_suggestions,
                "player_lookup": player_lookup,
                "slot_entries": slot_entries,
                "honor_categories": NBA_HONOR_CATEGORIES,
                "error_message": " ".join(error_messages),
                "status_message": None,
            },
        )

    _replace_user_nba_picks(
        current_user["uid"],
        playoff=playoff_payload,
        honors=honors_payload,
        all_nba=all_nba_payload,
    )
    return RedirectResponse(url="/nba-playoffs?saved=1", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/nba-playoffs/all", response_class=HTMLResponse)
def nba_playoffs_all_picks(request: Request, current_user: SessionUser = Depends(require_user)):
    picks = _load_all_users_nba_picks()
    slot_entries = [
        {"slot": slot, "label": data["label"], "bucket": data["bucket"]}
        for slot, data in NBA_ALL_NBA_SLOT_DEFS.items()
    ]
    return templates.TemplateResponse(
        "nba_playoffs_overview.html",
        {
            "request": request,
            "season_year": NBA_TARGET_SEASON_YEAR,
            "picks": picks,
            "conferences": NBA_CONFERENCES,
            "slot_entries": slot_entries,
            "honor_categories": NBA_HONOR_CATEGORIES,
        },
    )

# Test routes removed - implementing proper v2 system

@app.get("/hall-of-hate", response_class=HTMLResponse)
def hall_of_hate_view(request: Request, current_user: SessionUser | None = Depends(optional_user)):
    """Hall of Hate v2 - Uses proper v2 data and authentication"""
    if not current_user:
        return RedirectResponse(url="/login")
    
    # Get v2 villains with calculated averages
    villains = _get_hall_of_hate_entries(current_user)
    
    return templates.TemplateResponse(
        "hall_of_hate.html",
        {
            "request": request,
            "villains": villains,
            "current_user": current_user,
            "frame_configs": HALL_OF_HATE_V2_FRAMES,
            "v2_frames": HALL_OF_HATE_V2_FRAMES,
        }
    )





@app.get("/hall-of-hate/nuevo", response_class=HTMLResponse)
def hall_of_hate_new(request: Request, current_user: SessionUser = Depends(require_user)):
    """Add new villain to Hall of Hate v2"""
    # Load available frames from JSON configuration
    v2_frames = _load_v2_frame_definitions()
    available_frames = list(v2_frames.keys())
    
    return templates.TemplateResponse(
        "hall_of_hate_new.html",
        {
            "request": request,
            "current_user": current_user,
            "available_frames": available_frames,
            "v2_frames": v2_frames,
        }
    )

def _get_all_ldap_user_ids():
    """Get all LDAP user IDs for automatic rating assignment"""
    # Temporarily disabled LDAP connection for troubleshooting
    print("Warning: LDAP connection temporarily disabled for troubleshooting")
    user_ids = ["javi", "hugo", "isma"]
    return user_ids
    
    # Original LDAP code (commented out for troubleshooting)
    # from ldap3 import Server, Connection, SUBTREE, ALL
    # from app.core.config import settings
    # 
    # server = Server(settings.ldap_uri, get_info=ALL)
    # user_ids = []
    # 
    # try:
    #     with Connection(server, settings.ldap_bind_dn, settings.ldap_bind_password, auto_bind=True) as conn:
    #         conn.search(
    #             search_base=settings.ldap_base_dn,
    #             search_filter="(objectClass=inetOrgPerson)",
    #             search_scope=SUBTREE,
    #             attributes=["uid"]
    #         )
    #         entries = list(conn.entries)
    #         for entry in entries:
    #             uid = str(entry.uid) if "uid" in entry else ""
    #             if uid:
    #                 user_ids.append(uid)
    # except Exception as e:
    #     print(f"Warning: Could not fetch LDAP users for automatic ratings: {e}")
    #     # Return some default users if LDAP fails
    #     user_ids = ["javi", "hugo", "isma"]
    # 
    # return user_ids

def _cleanup_orphaned_ratings():
    """Remove ratings from users that no longer exist in LDAP"""
    # Temporarily disabled for troubleshooting
    print("[CLEANUP] Cleanup function temporarily disabled for troubleshooting")
    return
    
    # Original cleanup code (commented out for troubleshooting)
    # global pool
    # if not pool:
    #     print("Warning: Database connection not available for cleanup")
    #     return
    # 
    # try:
    #     # Get current LDAP users
    #     valid_users = _get_all_ldap_user_ids()
    #     print(f"[CLEANUP] Valid LDAP users: {valid_users}")
    #     
    #     conn = pool.getconn()
    #     try:
    #         with conn.cursor() as cur:
    #             # Get all users who have ratings
    #             cur.execute("SELECT DISTINCT user_name FROM hall_of_hate_v2_ratings")
    #             rating_users = [row[0] for row in cur.fetchall()]
    #             print(f"[CLEANUP] Users with ratings: {rating_users}")
    #             
    #             # Find orphaned users (users with ratings but not in LDAP)
    #             orphaned_users = [user for user in rating_users if user not in valid_users]
    #             
    #             if orphaned_users:
    #                 print(f"[CLEANUP] Found orphaned users: {orphaned_users}")
    #                 # Delete ratings for orphaned users
    #                 for user in orphaned_users:
    #                     cur.execute(
    #                         "DELETE FROM hall_of_hate_v2_ratings WHERE user_name = %s",
    #                         (user,)
    #                     )
    #                     deleted_count = cur.rowcount
    #                     print(f"[CLEANUP] Deleted {deleted_count} ratings for user '{user}'")
    #                 
    #                 conn.commit()
    #                 print(f"[CLEANUP] Successfully cleaned up ratings for {len(orphaned_users)} orphaned users")
    #             else:
    #                 print("[CLEANUP] No orphaned ratings found")
    #     finally:
    #         pool.putconn(conn)
    #         
    # except Exception as e:
    #     print(f"[CLEANUP] Error during orphaned ratings cleanup: {e}")

@app.post("/hall-of-hate/nuevo")
async def hall_of_hate_create(
    request: Request,
    name: str = Form(...),
    frame_type: str = Form("default"),
    image: UploadFile = File(...),
    current_user: SessionUser = Depends(require_user)
):
    """Create new villain in Hall of Hate v2"""
    if not image.content_type.startswith('image/'):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    if len(await image.read()) > HALL_OF_HATE_MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=400, detail="File too large")
    
    await image.seek(0)  # Reset file pointer after reading size
    
    # Save image
    file_extension = image.filename.split('.')[-1] if '.' in image.filename else 'jpg'
    safe_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')
    filename = f"{safe_name}.{file_extension}"
    
    upload_path = PathlibPath("app/images/hall_of_hate/uploads")
    upload_path.mkdir(exist_ok=True)
    
    file_path = upload_path / filename
    with open(file_path, "wb") as f:
        content = await image.read()
        f.write(content)
    
    # Save to database
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            print(f"[DEBUG] Creating villain: name='{name}', frame_type='{frame_type}', filename='uploads/{filename}'")
            # Create the villain
            cur.execute(
                """
                INSERT INTO hall_of_hate_v2 (name, image_filename, frame_type)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (name, f"uploads/{filename}", frame_type)
            )
            villain_id = cur.fetchone()[0]
            print(f"[DEBUG] Villain created with ID: {villain_id}")
            
            # Get all LDAP users and create automatic 99 ratings
            print(f"[DEBUG] Getting LDAP users for automatic ratings...")
            user_ids = _get_all_ldap_user_ids()
            print(f"[DEBUG] Found {len(user_ids)} LDAP users: {user_ids}")
            
            for user_id in user_ids:
                print(f"[DEBUG] Creating rating for user: {user_id}")
                cur.execute(
                    """
                    INSERT INTO hall_of_hate_v2_ratings (villain_id, user_name, rating)
                    VALUES (%s, %s, 99)
                    ON CONFLICT (villain_id, user_name) DO NOTHING
                    """,
                    (villain_id, user_id)
                )
            
            print(f"Created villain '{name}' with automatic 99 ratings for {len(user_ids)} users")
        conn.commit()
        print(f"[DEBUG] Successfully created villain '{name}' with automatic ratings")
    except psycopg2.IntegrityError as e:
        print(f"[DEBUG] IntegrityError: {e}")
        conn.rollback()
        raise HTTPException(status_code=400, detail="Villain name already exists")
    except Exception as e:
        print(f"[DEBUG] Unexpected error creating villain: {e}")
        print(f"[DEBUG] Error type: {type(e)}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        pool.putconn(conn)
    
    return RedirectResponse(url="/hall-of-hate", status_code=303)


# Hall of Hate v2 TEST Routes (No Authentication Required)
# Hall of Hate v2 Edit and Rate Routes
@app.get("/hall-of-hate/{villain_id}/edit", response_class=HTMLResponse)
def hall_of_hate_edit_view(
    request: Request,
    villain_id: int,
    current_user: SessionUser = Depends(require_user)
):
    """Edit villain in Hall of Hate v2"""
    global pool
    if not pool:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, image_filename, frame_type FROM hall_of_hate_v2 WHERE id = %s",
            (villain_id,)
        )
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Villain not found")
        
        db_id, name, image_filename, frame_type = result
        
        # Get average hate score
        cursor.execute(
            "SELECT COALESCE(AVG(rating), 99) FROM hall_of_hate_v2_ratings WHERE villain_id = %s",
            (villain_id,)
        )
        avg_result = cursor.fetchone()
        average_hate = avg_result[0] if avg_result else 99
        
        villain_data = {
            "id": db_id,
            "name": name,
            "image_filename": image_filename,
            "frame_type": frame_type or "default",
            "hate_score": int(average_hate)
        }
    finally:
        pool.putconn(conn)
    
    # Load available frames from JSON configuration
    v2_frames = _load_v2_frame_definitions()
    available_frames = list(v2_frames.keys())
    
    return templates.TemplateResponse(
        "hall_of_hate_edit.html",
        {
            "request": request,
            "villain": villain_data,
            "current_user": current_user,
            "available_frames": available_frames,
            "v2_frames": v2_frames,
        }
    )

@app.post("/hall-of-hate/{villain_id}/edit")
async def hall_of_hate_edit_update(
    request: Request,
    villain_id: int,
    name: str = Form(...),
    frame_type: str = Form("default"),
    image: UploadFile = File(None),
    current_user: SessionUser = Depends(require_user)
):
    """Update villain in Hall of Hate v2"""
    global pool
    if not pool:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        
        # Verify villain exists
        cursor.execute("SELECT id, image_filename FROM hall_of_hate_v2 WHERE id = %s", (villain_id,))
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Villain not found")
        
        current_image = result[1]
        new_image_filename = current_image
        
        # Handle image upload if provided
        if image and image.filename:
            # Validate image type
            if not image.content_type.startswith('image/'):
                raise HTTPException(status_code=400, detail="Invalid image file")
            
            # Generate filename and save
            file_extension = image.filename.split('.')[-1]
            filename_only = f"{name.replace(' ', '_')}.{file_extension}"
            new_image_filename = f"uploads/{filename_only}"  # Store relative path in DB
            
            uploads_dir = PathlibPath("app/images/hall_of_hate/uploads")
            uploads_dir.mkdir(exist_ok=True)
            
            # Delete old image if it exists and is in uploads folder
            if current_image and current_image.startswith('uploads/'):
                old_file_path = PathlibPath("app/images/hall_of_hate") / current_image
                if old_file_path.exists():
                    old_file_path.unlink()
            
            file_path = uploads_dir / filename_only
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(image.file, buffer)
        
        # Update database
        cursor.execute("""
            UPDATE hall_of_hate_v2 
            SET name = %s, frame_type = %s, image_filename = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (name, frame_type, new_image_filename, villain_id))
        
        conn.commit()
    finally:
        pool.putconn(conn)
    
    return RedirectResponse(url="/hall-of-hate", status_code=303)

@app.get("/hall-of-hate/{villain_id}/rate", response_class=HTMLResponse)
def hall_of_hate_rate_view(
    request: Request,
    villain_id: int,
    current_user: SessionUser = Depends(require_user)
):
    """Rate villain in Hall of Hate v2"""
    global pool
    if not pool:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, image_filename, frame_type FROM hall_of_hate_v2 WHERE id = %s",
            (villain_id,)
        )
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Villain not found")
        
        db_id, name, image_filename, frame_type = result
        
        # Get current user's rating if exists
        user_id = current_user.get("id") or current_user.get("uid")
        current_rating = 50  # Default middle value
        if user_id:
            cursor.execute(
                "SELECT rating FROM hall_of_hate_v2_ratings WHERE villain_id = %s AND user_name = %s",
                (villain_id, user_id)
            )
            rating_result = cursor.fetchone()
            if rating_result:
                current_rating = rating_result[0]
        
        # Get average hate score
        cursor.execute(
            "SELECT COALESCE(AVG(rating), 99) FROM hall_of_hate_v2_ratings WHERE villain_id = %s",
            (villain_id,)
        )
        avg_result = cursor.fetchone()
        average_hate = avg_result[0] if avg_result else 99
        
        villain_data = {
            "id": db_id,
            "name": name,
            "image_filename": image_filename,
            "frame_type": frame_type or "default",
            "hate_score": int(average_hate),
            "user_rating": current_rating
        }
    finally:
        pool.putconn(conn)
    
    return templates.TemplateResponse(
        "hall_of_hate_rate.html",
        {
            "request": request,
            "villain": villain_data,
            "current_user": current_user
        }
    )

@app.post("/hall-of-hate/{villain_id}/rate")
async def hall_of_hate_rate_submit(
    request: Request,
    villain_id: int,
    hate_rating: int = Form(..., ge=1, le=99),
    current_user: SessionUser = Depends(require_user)
):
    """Submit rating for villain in Hall of Hate v2"""
    global pool
    if not pool:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    # Get user identifier
    user_id = current_user.get("id") or current_user.get("uid")
    if not user_id:
        raise HTTPException(status_code=400, detail="User ID not available")
    
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        
        # Verify villain exists
        cursor.execute("SELECT id FROM hall_of_hate_v2 WHERE id = %s", (villain_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Villain not found")
        
        # Insert or update rating using UPSERT
        cursor.execute("""
            INSERT INTO hall_of_hate_v2_ratings (villain_id, user_name, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (villain_id, user_name)
            DO UPDATE SET rating = EXCLUDED.rating
        """, (villain_id, user_id, hate_rating))
        
        conn.commit()
    finally:
        pool.putconn(conn)
    
    return RedirectResponse(url="/hall-of-hate", status_code=303)

@app.post("/hall-of-hate/{villain_id}/delete")
async def hall_of_hate_delete(
    request: Request,
    villain_id: int,
    current_user: SessionUser = Depends(require_user)
):
    """Delete villain from Hall of Hate"""
    global pool
    if not pool:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        
        # Get villain info before deletion for cleanup
        cursor.execute("SELECT image_filename FROM hall_of_hate_v2 WHERE id = %s", (villain_id,))
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Villain not found")
        
        image_filename = result[0]
        
        # Delete from database (cascade will handle ratings)
        cursor.execute("DELETE FROM hall_of_hate_v2 WHERE id = %s", (villain_id,))
        conn.commit()
        
        # Try to delete image file
        try:
            import os
            image_path = os.path.join("/app/app/images/hall_of_hate/uploads", image_filename)
            print(f"Attempting to delete image file: {image_path}")
            if os.path.exists(image_path):
                os.remove(image_path)
                print(f"Successfully deleted image file: {image_filename}")
            else:
                print(f"Image file not found: {image_path}")
        except Exception as e:
            # Log but don't fail the deletion if file cleanup fails
            print(f"Warning: Could not delete image file {image_filename}: {e}")
        
    finally:
        pool.putconn(conn)
    
    return RedirectResponse(url="/hall-of-hate", status_code=303)


@app.get("/bets", response_class=HTMLResponse)
def bets_home(request: Request, current_user: SessionUser = Depends(require_user)):
    is_admin = current_user["is_admin"]
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, apuesta, creacion, categoria, tipo, multiplica,
                       apostante1, apostante2, apostante3,
                       apostado1, apostado2, apostado3,
                       ganador1, ganador2, perdedor1, perdedor2,
                       locked, resultado_registrado, auto_lock_released
                FROM apuestas
                ORDER BY id DESC
            """)
            rows = cur.fetchall()
    finally:
        pool.putconn(conn)

    apuestas = [
        {
            "id": r[0], "apuesta": r[1], "creacion": r[2], "categoria": r[3], "tipo": r[4],
            "multiplica": r[5],
            "apostante1": r[6], "apostante2": r[7], "apostante3": r[8],
            "apostado1": r[9], "apostado2": r[10], "apostado3": r[11],
            "ganador1": r[12], "ganador2": r[13], "perdedor1": r[14], "perdedor2": r[15],
            "locked": bool(r[16]),
            "resultado_registrado": r[17],
            "auto_lock_released": bool(r[18]),
        } for r in rows
    ]

    for apuesta in apuestas:
        winners = (apuesta["ganador1"], apuesta["ganador2"])
        losers = (apuesta["perdedor1"], apuesta["perdedor2"])
        auto_locked, effective_locked = _compute_auto_locked(
            apuesta["locked"],
            apuesta["resultado_registrado"],
            apuesta["auto_lock_released"],
            winners,
            losers,
        )
        apuesta["auto_locked"] = auto_locked
        apuesta["effective_locked"] = effective_locked
        apuesta["estado_label"] = _compute_estado_label(winners, losers)

    return templates.TemplateResponse(
        "bets.html",
        {
            "request": request,
            "apuestas": apuestas,
            "is_admin": is_admin,
        }
    )


@app.get("/apuestas/nueva", response_class=HTMLResponse)
def nueva_apuesta_form(request: Request, current_user: SessionUser = Depends(require_user)):
    usuarios = auth_ldap.fetch_all_user_uids()
    return templates.TemplateResponse(
        "add_apuesta.html",
        {
            "request": request,
            "usuarios": usuarios,
            "categorias": CATEGORIAS_PREDEFINIDAS,
            "multiplica_opciones": MULTIPLICA_OPCIONES,
            "is_admin": current_user["is_admin"],
        },
    )


@app.post("/apuestas/nueva")
def crear_apuesta(
    request: Request,
    apuesta: str = Form(...),
    categoria: str = Form(...),
    tipo: str = Form(...),
    multiplica: int = Form(...),

    apostante1: str | None = Form(None),
    apostante2: str | None = Form(None),
    apostante3: str | None = Form(None),

    apostado1: str | None = Form(None),
    apostado2: str | None = Form(None),
    apostado3: str | None = Form(None),

    ganador1: str | None = Form(None),
    ganador2: str | None = Form(None),
    perdedor1: str | None = Form(None),
    perdedor2: str | None = Form(None),
    current_user: SessionUser = Depends(require_user),
):
    _ = current_user  # enforce sesión activa
    clean_apostante1 = _empty_to_none(apostante1)
    clean_apostante2 = _empty_to_none(apostante2)
    clean_apostante3 = _empty_to_none(apostante3)
    clean_apostado1 = _empty_to_none(apostado1)
    clean_apostado2 = _empty_to_none(apostado2)
    clean_apostado3 = _empty_to_none(apostado3)
    clean_ganador1 = _empty_to_none(ganador1)
    clean_ganador2 = _empty_to_none(ganador2)
    clean_perdedor1 = _empty_to_none(perdedor1)
    clean_perdedor2 = _empty_to_none(perdedor2)

    winners = (clean_ganador1, clean_ganador2)
    losers = (clean_perdedor1, clean_perdedor2)
    has_result = _has_result_fields(winners, losers)
    resultado_registrado = date.today() if has_result else None

    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO apuestas (
                    apuesta, creacion, categoria, tipo, multiplica,
                    apostante1, apostante2, apostante3,
                    apostado1, apostado2, apostado3,
                    ganador1, ganador2, perdedor1, perdedor2,
                    locked, resultado_registrado, auto_lock_released
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                ) RETURNING id
            """, (
                apuesta,
                date.today(),
                categoria,
                tipo,
                multiplica,
                clean_apostante1,
                clean_apostante2,
                clean_apostante3,
                clean_apostado1,
                clean_apostado2,
                clean_apostado3,
                clean_ganador1,
                clean_ganador2,
                clean_perdedor1,
                clean_perdedor2,
                False,
                resultado_registrado,
                False,
            ))
            _new_id = cur.fetchone()[0]
    finally:
        pool.putconn(conn)

    return RedirectResponse(url="/bets", status_code=303)


@app.get("/clasificacion", response_class=HTMLResponse)
def clasificacion(request: Request, current_user: SessionUser = Depends(require_user)):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT multiplica,
                       categoria,
                       tipo,
                       apostante1, apostante2, apostante3,
                       ganador1, ganador2,
                       perdedor1, perdedor2
                FROM apuestas
                """
            )
            rows = cur.fetchall()
    finally:
        pool.putconn(conn)

    tipo_keys = ("largo", "unico")
    tipo_labels = {"largo": "Largo", "unico": "Unico"}

    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "apuestados": 0,
            "ganados": 0,
            "ganados_base": 0,
            "perdidos": 0,
        }
    )
    category_totals: dict[str, dict[str, int]] = defaultdict(lambda: {key: 0 for key in tipo_keys})
    players_by_category: dict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
    played_by_type = {key: defaultdict(lambda: defaultdict(int)) for key in tipo_keys}
    wins_by_type = {key: defaultdict(lambda: defaultdict(int)) for key in tipo_keys}
    losses_by_type = {key: defaultdict(lambda: defaultdict(int)) for key in tipo_keys}
    categories_seen: set[str] = set()

    def _normalize_categoria(value: str | None) -> str:
        if value is None:
            return "Sin categoria"
        cleaned = value.strip()
        return cleaned or "Sin categoria"

    def _normalize_tipo(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip().lower()
        if cleaned in {"largo", "larga"}:
            return "largo"
        if cleaned in {"unico", "único", "corta", "corto", "unica"}:
            return "unico"
        return None

    for row in rows:
        multiplica = row[0] or 0
        categoria = _normalize_categoria(row[1])
        tipo_key = _normalize_tipo(row[2])
        apostantes = row[3:6]
        ganadores = row[6:8]
        perdedores = row[8:10]

        categories_seen.add(categoria)
        if tipo_key:
            category_totals[categoria][tipo_key] += 1

        for apostante in apostantes:
            nombre = _empty_to_none(apostante)
            if not nombre:
                continue
            stats[nombre]["apuestados"] += 1
            players_by_category[nombre][categoria] += 1
            if tipo_key:
                played_by_type[tipo_key][nombre][categoria] += 1

        for ganador in ganadores:
            nombre = _empty_to_none(ganador)
            if not nombre:
                continue
            stats[nombre]["ganados"] += multiplica
            stats[nombre]["ganados_base"] += 1
            if tipo_key:
                wins_by_type[tipo_key][nombre][categoria] += 1

        for perdedor in perdedores:
            nombre = _empty_to_none(perdedor)
            if not nombre:
                continue
            stats[nombre]["perdidos"] += 1
            if tipo_key:
                losses_by_type[tipo_key][nombre][categoria] += 1

    usuarios_ldap = auth_ldap.fetch_all_user_uids()

    for usuario in usuarios_ldap:
        _ = stats[usuario]
        _ = players_by_category[usuario]
        for tipo_key in tipo_keys:
            _ = played_by_type[tipo_key][usuario]
            _ = wins_by_type[tipo_key][usuario]
            _ = losses_by_type[tipo_key][usuario]

    categories_seen.update(CATEGORIAS_PREDEFINIDAS)
    category_order: list[str] = []
    seen_categories: set[str] = set()
    for categoria in CATEGORIAS_PREDEFINIDAS:
        if categoria not in seen_categories:
            category_order.append(categoria)
            seen_categories.add(categoria)
    for categoria in sorted(categories_seen):
        if categoria not in seen_categories:
            category_order.append(categoria)
            seen_categories.add(categoria)

    category_summary: list[dict[str, int | str]] = []
    totals_row = {"categoria": "Total", "largo": 0, "unico": 0, "total": 0}
    for categoria in category_order:
        counts = category_totals.get(categoria, {key: 0 for key in tipo_keys})
        largo_val = counts.get("largo", 0)
        unico_val = counts.get("unico", 0)
        total_val = largo_val + unico_val
        category_summary.append(
            {
                "categoria": categoria,
                "largo": largo_val,
                "unico": unico_val,
                "total": total_val,
            }
        )
        totals_row["largo"] += largo_val
        totals_row["unico"] += unico_val
    totals_row["total"] = totals_row["largo"] + totals_row["unico"]

    def _build_player_table(source: dict[str, dict[str, int]]) -> list[dict[str, object]]:
        rows_out: list[dict[str, object]] = []
        for nombre in sorted(source.keys(), key=str.lower):
            category_counts = [source[nombre].get(cat, 0) for cat in category_order]
            total_count = sum(category_counts)
            if total_count == 0:
                continue
            rows_out.append({
                "nombre": nombre,
                "counts": category_counts,
                "total": total_count,
            })
        return rows_out

    player_category_rows = _build_player_table(players_by_category)
    type_tables = []
    for tipo_key in tipo_keys:
        type_tables.append({
            "key": tipo_key,
            "label": tipo_labels[tipo_key],
            "jugados": _build_player_table(played_by_type[tipo_key]),
            "ganados": _build_player_table(wins_by_type[tipo_key]),
            "perdidos": _build_player_table(losses_by_type[tipo_key]),
        })

    clasificacion_datos = []
    for nombre, datos in stats.items():
        ganados = datos["ganados"]
        perdidos = datos["perdidos"]
        balance = ganados - perdidos
        pendientes = datos["apuestados"] - (datos["ganados_base"] + datos["perdidos"])
        clasificacion_datos.append(
            {
                "nombre": nombre,
                "apuestados": datos["apuestados"],
                "ganados": ganados,
                "perdidos": perdidos,
                "balance": balance,
                "pendientes": pendientes,
            }
        )

    clasificacion_datos.sort(
        key=lambda item: (
            -item["balance"],
            -item["ganados"],
            item["perdidos"],
            item["nombre"].lower(),
        )
    )

    for idx, fila in enumerate(clasificacion_datos, start=1):
        fila["posicion"] = idx

    return templates.TemplateResponse(
        "clasificacion.html",
        {
            "request": request,
            "clasificacion": clasificacion_datos,
            "usuarios": usuarios_ldap,
            "category_order": category_order,
            "category_summary": category_summary,
            "category_summary_totals": totals_row,
            "player_category_rows": player_category_rows,
            "type_tables": type_tables,
            "is_admin": current_user["is_admin"],
        },
    )

@app.post("/apuestas/{apuesta_id}/borrar")
def borrar_apuesta(
    request: Request,
    apuesta_id: int = Path(...),
    current_user: SessionUser = Depends(require_user),
):
    is_admin = current_user["is_admin"]
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT locked, resultado_registrado, auto_lock_released,
                       ganador1, ganador2, perdedor1, perdedor2
                FROM apuestas
                WHERE id = %s
                """,
                (apuesta_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Apuesta no encontrada")

        locked = bool(row[0])
        _auto_locked, effective_locked = _compute_auto_locked(
            locked,
            row[1],
            bool(row[2]),
            (row[3], row[4]),
            (row[5], row[6]),
        )
        if effective_locked and not is_admin:
            raise HTTPException(status_code=403, detail="La apuesta está bloqueada para borrado")

        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM apuestas WHERE id = %s", (apuesta_id,))
    finally:
        pool.putconn(conn)
    return RedirectResponse(url="/bets", status_code=303)


@app.get("/apuestas/{apuesta_id}/editar", response_class=HTMLResponse)
def editar_apuesta_form(
    request: Request,
    apuesta_id: int = Path(...),
    current_user: SessionUser = Depends(require_user),
):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, apuesta, categoria, tipo, multiplica,
                       apostante1, apostante2, apostante3,
                       apostado1, apostado2, apostado3,
                       ganador1, ganador2, perdedor1, perdedor2,
                       locked, resultado_registrado, auto_lock_released
                FROM apuestas
                WHERE id = %s
                """,
                (apuesta_id,),
            )
            row = cur.fetchone()
    finally:
        pool.putconn(conn)

    if not row:
        raise HTTPException(status_code=404, detail="Apuesta no encontrada")

    apuesta = {
        "id": row[0],
        "apuesta": row[1],
        "categoria": row[2],
        "tipo": row[3],
        "multiplica": row[4],
        "apostante1": row[5],
        "apostante2": row[6],
        "apostante3": row[7],
        "apostado1": row[8],
        "apostado2": row[9],
        "apostado3": row[10],
        "ganador1": row[11],
        "ganador2": row[12],
        "perdedor1": row[13],
        "perdedor2": row[14],
        "locked": bool(row[15]),
        "resultado_registrado": row[16],
        "auto_lock_released": bool(row[17]),
    }

    usuarios = auth_ldap.fetch_all_user_uids()

    winners = (apuesta["ganador1"], apuesta["ganador2"])
    losers = (apuesta["perdedor1"], apuesta["perdedor2"])
    auto_locked, effective_locked = _compute_auto_locked(
        apuesta["locked"],
        apuesta["resultado_registrado"],
        apuesta["auto_lock_released"],
        winners,
        losers,
    )

    is_admin = current_user["is_admin"]
    if effective_locked and not is_admin:
        raise HTTPException(status_code=403, detail="La apuesta está bloqueada")

    apuesta["auto_locked"] = auto_locked
    apuesta["effective_locked"] = effective_locked
    apuesta["estado_label"] = _compute_estado_label(winners, losers)

    return templates.TemplateResponse(
        "edit_apuesta.html",
        {
            "request": request,
            "apuesta": apuesta,
            "usuarios": usuarios,
            "categorias": CATEGORIAS_PREDEFINIDAS,
            "multiplica_opciones": MULTIPLICA_OPCIONES,
            "is_admin": is_admin,
        },
    )

@app.post("/apuestas/{apuesta_id}/editar")
def actualizar_apuesta(
    request: Request,
    apuesta_id: int = Path(...),
    apuesta: str = Form(...),
    categoria: str = Form(...),
    tipo: str = Form(...),
    multiplica: int = Form(...),
    apostante1: str | None = Form(None),
    apostante2: str | None = Form(None),
    apostante3: str | None = Form(None),
    apostado1: str | None = Form(None),
    apostado2: str | None = Form(None),
    apostado3: str | None = Form(None),
    ganador1: str | None = Form(None),
    ganador2: str | None = Form(None),
    perdedor1: str | None = Form(None),
    perdedor2: str | None = Form(None),
    bloqueo: str | None = Form(None),
    current_user: SessionUser = Depends(require_user),
):
    is_admin = current_user["is_admin"]
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT locked, resultado_registrado, auto_lock_released,
                       ganador1, ganador2, perdedor1, perdedor2
                FROM apuestas
                WHERE id = %s
                """,
                (apuesta_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Apuesta no encontrada")

        current_locked = bool(row[0])
        result_recorded = row[1]
        auto_lock_released = bool(row[2])
        prev_winners = (row[3], row[4])
        prev_losers = (row[5], row[6])

        auto_locked, effective_locked = _compute_auto_locked(
            current_locked,
            result_recorded,
            auto_lock_released,
            prev_winners,
            prev_losers,
        )
        if effective_locked and not is_admin:
            raise HTTPException(status_code=403, detail="La apuesta está bloqueada para edición")

        clean_apostante1 = _empty_to_none(apostante1)
        clean_apostante2 = _empty_to_none(apostante2)
        clean_apostante3 = _empty_to_none(apostante3)
        clean_apostado1 = _empty_to_none(apostado1)
        clean_apostado2 = _empty_to_none(apostado2)
        clean_apostado3 = _empty_to_none(apostado3)
        clean_ganador1 = _empty_to_none(ganador1)
        clean_ganador2 = _empty_to_none(ganador2)
        clean_perdedor1 = _empty_to_none(perdedor1)
        clean_perdedor2 = _empty_to_none(perdedor2)

        new_winners = (clean_ganador1, clean_ganador2)
        new_losers = (clean_perdedor1, clean_perdedor2)

        prev_clean_winners = tuple(_empty_to_none(item) for item in prev_winners)
        prev_clean_losers = tuple(_empty_to_none(item) for item in prev_losers)
        prev_has_result = _has_result_fields(prev_clean_winners, prev_clean_losers)
        new_has_result = _has_result_fields(new_winners, new_losers)
        result_changed = (new_winners != prev_clean_winners) or (new_losers != prev_clean_losers)

        new_result_recorded = result_recorded
        new_auto_lock_released = auto_lock_released
        if new_has_result:
            if not prev_has_result or result_changed:
                new_result_recorded = date.today()
                new_auto_lock_released = False
        else:
            new_result_recorded = None
            new_auto_lock_released = False

        desired_locked = current_locked
        if is_admin:
            desired_locked = _parse_locked_value(bloqueo, current_locked)
            if desired_locked:
                new_auto_lock_released = False
            elif new_has_result:
                new_auto_lock_released = True

        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE apuestas SET
                    apuesta = %s,
                    categoria = %s,
                    tipo = %s,
                    multiplica = %s,
                    apostante1 = %s,
                    apostante2 = %s,
                    apostante3 = %s,
                    apostado1 = %s,
                    apostado2 = %s,
                    apostado3 = %s,
                    ganador1 = %s,
                    ganador2 = %s,
                    perdedor1 = %s,
                    perdedor2 = %s,
                    locked = %s,
                    resultado_registrado = %s,
                    auto_lock_released = %s
                WHERE id = %s
                """,
                (
                    apuesta,
                    categoria,
                    tipo,
                    multiplica,
                    clean_apostante1,
                    clean_apostante2,
                    clean_apostante3,
                    clean_apostado1,
                    clean_apostado2,
                    clean_apostado3,
                    clean_ganador1,
                    clean_ganador2,
                    clean_perdedor1,
                    clean_perdedor2,
                    desired_locked,
                    new_result_recorded,
                    new_auto_lock_released,
                    apuesta_id,
                ),
            )
    finally:
        pool.putconn(conn)

    return RedirectResponse(url="/bets", status_code=303)

# Admin Utility Endpoints
@app.post("/admin/cleanup-orphaned-ratings")
async def cleanup_orphaned_ratings_endpoint(
    request: Request,
    current_user: SessionUser = Depends(require_admin)
):
    """Admin endpoint to clean up ratings from deleted LDAP users"""
    try:
        _cleanup_orphaned_ratings()
        return {"status": "success", "message": "Orphaned ratings cleanup completed"}
    except Exception as e:
        print(f"Error in cleanup endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

@app.delete("/admin/user/{username}/ratings")
async def delete_user_ratings(
    username: str,
    request: Request,
    current_user: SessionUser = Depends(require_admin)
):
    """Admin endpoint to delete all ratings for a specific user"""
    global pool
    if not pool:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            # Delete all ratings for the specified user
            cur.execute(
                "DELETE FROM hall_of_hate_v2_ratings WHERE user_name = %s",
                (username,)
            )
            deleted_count = cur.rowcount
            conn.commit()
            
            return {
                "status": "success", 
                "message": f"Deleted {deleted_count} ratings for user '{username}'"
            }
    except Exception as e:
        print(f"Error deleting user ratings: {e}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete ratings: {str(e)}")
    finally:
        pool.putconn(conn)
