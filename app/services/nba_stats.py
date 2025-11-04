# app/services/nba_stats.py
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
from nba_api.stats.endpoints import (
    leaguedashteamstats,
    leaguedashplayerstats,
    leaguestandingsv3,
)
import requests
import os
from app.services.nba_headers import attach_to_session, ensure_nba_api_headers

SEASON = os.getenv("NBA_SEASON", "2025-26")  # formato 'YYYY-YY', p.e. '2025-26'
CACHE_TTL = int(os.getenv("NBA_CACHE_TTL_SECONDS", "900"))  # 15 min

# Sesión con headers realistas para evitar bloqueos de nba.com/stats
ensure_nba_api_headers()
session = requests.Session()
attach_to_session(session)
HTTP_TIMEOUT = 15

_cache: Dict[str, Dict] = {}

def _now():
    return datetime.utcnow()

def _get_cache(key: str):
    hit = _cache.get(key)
    if hit and hit["expires_at"] > _now():
        return hit["value"]
    return None

def _set_cache(key: str, value):
    _cache[key] = {"value": value, "expires_at": _now() + timedelta(seconds=CACHE_TTL)}

def _zscore(s: pd.Series) -> pd.Series:
    if s.std(ddof=0) == 0:
        return s * 0
    return (s - s.mean()) / s.std(ddof=0)

def get_team_advanced() -> List[Dict]:
    """
    TOP10 por Net Rating con métricas avanzadas.
    """
    cache_key = f"team_adv_{SEASON}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    try:
        df = leaguedashteamstats.LeagueDashTeamStats(
            season=SEASON,
            measure_type_detailed_defense="Advanced",
            per_mode_detailed="PerGame",
            season_type_all_star="Regular Season",
            league_id_nullable="00",
            headers=session.headers,
            timeout=HTTP_TIMEOUT
        ).get_data_frames()[0]
    except Exception as exc:
        print(f"[NBA] team_advanced fetch failed: {exc}")
        return cached if cached is not None else []

    df = df[df["TEAM_ID"].astype(str).str.startswith("161061")]  # solo franquicias NBA

    cols = [
        "TEAM_ID",
        "TEAM_NAME",
        "GP",
        "W",
        "L",
        "W_PCT",
        "OFF_RATING",
        "DEF_RATING",
        "NET_RATING",
        "PACE",
        "TS_PCT",
        "EFG_PCT",
        "OREB_PCT",
        "DREB_PCT",
        "TM_TOV_PCT",
    ]
    available = [c for c in cols if c in df.columns]
    df = df[available].sort_values("NET_RATING", ascending=False)
    top10 = df.head(10).to_dict(orient="records")
    _set_cache(cache_key, top10)
    return top10

def get_mvp_ladder() -> List[Dict]:
    """
    Heurística simple y transparente para MVP:
    MVP_score = z(PTS) + 1.2*z(AST) + 0.8*z(REB) + 1.5*z(TS%) + 1.8*z(TEAM_WPCT)
    (mezcla producción individual y rendimiento del equipo)
    """
    cache_key = f"mvp_{SEASON}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    # Producción individual (Advanced para TS%)
    try:
        advanced_raw = leaguedashplayerstats.LeagueDashPlayerStats(
            season=SEASON,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Advanced",
            headers=session.headers,
            timeout=HTTP_TIMEOUT
        ).get_data_frames()[0]
        advanced = advanced_raw[[
            "TEAM_ID",
            "PLAYER_ID",
            "PLAYER_NAME",
            "TEAM_ABBREVIATION",
            "GP",
            "W",
            "L",
            "W_PCT",
            "TS_PCT",
        ]]
        base = leaguedashplayerstats.LeagueDashPlayerStats(
            season=SEASON,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Base",
            season_type_all_star="Regular Season",
            league_id_nullable="00",
            headers=session.headers,
            timeout=HTTP_TIMEOUT
        ).get_data_frames()[0][["PLAYER_ID", "PTS", "AST", "REB"]]
        p = advanced.merge(base, on="PLAYER_ID", how="left")
    except Exception as exc:
        print(f"[NBA] mvp ladder fetch failed: {exc}")
        return cached if cached is not None else []

    # Win% del equipo
    st_raw = leaguestandingsv3.LeagueStandingsV3(
        season=SEASON,
        league_id="00",
        season_type="Regular Season",
        headers=session.headers,
        timeout=HTTP_TIMEOUT
    ).get_data_frames()[0]
    if {"W", "L"}.issubset(st_raw.columns):
        standings_cols = {"TeamID": "TEAM_ID", "W": "W", "L": "L", "WinPCT": "TEAM_WPCT"}
    else:
        standings_cols = {"TeamID": "TEAM_ID", "WINS": "W", "LOSSES": "L", "WinPCT": "TEAM_WPCT"}
    available_cols = [c for c in standings_cols if c in st_raw.columns]
    st = st_raw[available_cols].rename(columns=standings_cols)
    st = st[["TEAM_ID", "TEAM_WPCT"]]

    df = p.merge(st, on="TEAM_ID", how="left")
    safe = df.fillna({"TEAM_WPCT": 0.5})
    # columnas que usaremos
    pick = safe[["PLAYER_ID","PLAYER_NAME","TEAM_ABBREVIATION","GP","W","L","TEAM_WPCT","PTS","AST","REB","TS_PCT"]].copy()

    # zscores
    for c in ["PTS","AST","REB","TS_PCT","TEAM_WPCT"]:
        pick[f"z_{c}"] = _zscore(pick[c])

    pick["MVP_SCORE"] = pick["z_PTS"] + 1.2*pick["z_AST"] + 0.8*pick["z_REB"] + 1.5*pick["z_TS_PCT"] + 1.8*pick["z_TEAM_WPCT"]
    pick = pick.sort_values("MVP_SCORE", ascending=False)

    cols_out = ["PLAYER_ID","PLAYER_NAME","TEAM_ABBREVIATION","GP","PTS","AST","REB","TS_PCT","TEAM_WPCT","MVP_SCORE"]
    top10 = pick[cols_out].head(10).to_dict(orient="records")
    _set_cache(cache_key, top10)
    return top10

def get_roy_ladder() -> List[Dict]:
    """
    ROY = mismos ingredientes pero filtrando rookies.
    ROY_score = z(PTS) + 1.0*z(AST) + 1.0*z(REB) + 1.2*z(TS%)
    (no metemos Win% del equipo para no penalizar al rookie por contexto)
    """
    cache_key = f"roy_{SEASON}"
    cached = _get_cache(cache_key)
    if cached is not None:
        return cached

    try:
        rook_adv_raw = leaguedashplayerstats.LeagueDashPlayerStats(
            season=SEASON,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Advanced",
            player_experience_nullable="Rookie",
            season_type_all_star="Regular Season",
            league_id_nullable="00",
            headers=session.headers,
            timeout=HTTP_TIMEOUT
        ).get_data_frames()[0]
        rook_adv = rook_adv_raw[[
            "TEAM_ID",
            "PLAYER_ID",
            "PLAYER_NAME",
            "TEAM_ABBREVIATION",
            "GP",
            "W",
            "L",
            "W_PCT",
            "TS_PCT",
        ]]
        rook_base = leaguedashplayerstats.LeagueDashPlayerStats(
            season=SEASON,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Base",
            player_experience_nullable="Rookie",
            season_type_all_star="Regular Season",
            league_id_nullable="00",
            headers=session.headers,
            timeout=HTTP_TIMEOUT
        ).get_data_frames()[0][["PLAYER_ID", "PTS", "AST", "REB"]]
        rook = rook_adv.merge(rook_base, on="PLAYER_ID", how="left")
    except Exception as exc:
        print(f"[NBA] roy ladder fetch failed: {exc}")
        return cached if cached is not None else []

    pick = rook[["PLAYER_ID","PLAYER_NAME","TEAM_ABBREVIATION","GP","PTS","AST","REB","TS_PCT"]].copy()
    for c in ["PTS","AST","REB","TS_PCT"]:
        pick[f"z_{c}"] = _zscore(pick[c])

    pick["ROY_SCORE"] = pick["z_PTS"] + pick["z_AST"] + pick["z_REB"] + 1.2*pick["z_TS_PCT"]
    cols_out = ["PLAYER_ID","PLAYER_NAME","TEAM_ABBREVIATION","GP","PTS","AST","REB","TS_PCT","ROY_SCORE"]
    top10 = pick.sort_values("ROY_SCORE", ascending=False)[cols_out].head(10).to_dict(orient="records")
    _set_cache(cache_key, top10)
    return top10
