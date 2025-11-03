#!/usr/bin/env python3
"""
Utility to populate NBA teams and players using nba_api.

Run inside the dev container:
    docker compose -f docker-compose.dev.yml exec corderos-app python scripts/fetch_nba_data.py
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Tuple

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.static import teams as nba_teams_static
from nba_api.stats.static import players as nba_players_static
from nba_api.stats.endpoints import commonteamroster


WEST_ABBREVIATIONS = {
    "DAL",
    "DEN",
    "GSW",
    "HOU",
    "LAC",
    "LAL",
    "MEM",
    "MIN",
    "NOP",
    "OKC",
    "PHX",
    "POR",
    "SAC",
    "SAS",
    "UTA",
}


def _conference_from_entry(entry: dict) -> str:
    """Derive the team conference using available metadata or a static map."""
    raw = entry.get("conference") or entry.get("confName")
    if raw:
        normalized = raw.strip().lower()
        if "west" in normalized:
            return "West"
        if "east" in normalized:
            return "East"
    abbr = (entry.get("abbreviation") or "").upper()
    if abbr in WEST_ABBREVIATIONS:
        return "West"
    return "East"


def upsert_teams(conn) -> Dict[int, int]:
    """Insert or update NBA teams and return a mapping nba_team_id -> internal id."""
    team_map: Dict[int, int] = {}
    payload = nba_teams_static.get_teams()
    if not payload:
        print("⚠️  nba_api.get_teams() devolvió una lista vacía", file=sys.stderr)
        return team_map

    with conn.cursor() as cur:
        for entry in payload:
            nba_team_id = entry.get("id")
            if nba_team_id is None:
                continue
            full_name = entry.get("full_name") or entry.get("nickname") or "Unknown"
            abbreviation = entry.get("abbreviation") or entry.get("tricode") or ""
            conference = _conference_from_entry(entry)
            city = entry.get("city")
            nickname = entry.get("nickname")
            cur.execute(
                """
                INSERT INTO nba_teams (nba_team_id, full_name, abbreviation, conference, city, nickname)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (nba_team_id) DO UPDATE
                SET full_name = EXCLUDED.full_name,
                    abbreviation = EXCLUDED.abbreviation,
                    conference = EXCLUDED.conference,
                    city = EXCLUDED.city,
                    nickname = EXCLUDED.nickname
                RETURNING id
                """,
                (nba_team_id, full_name, abbreviation, conference, city, nickname),
            )
            team_id = cur.fetchone()[0]
            team_map[int(nba_team_id)] = int(team_id)
    conn.commit()
    print(f"✅ Sincronizados {len(team_map)} equipos NBA")
    return team_map


def _fetch_player_metadata(team_map: Dict[int, int]) -> Dict[int, Tuple[str, int | None]]:
    metadata: Dict[int, Tuple[str, int | None]] = {}
    for nba_team_id, internal_team_id in team_map.items():
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=nba_team_id)
            data = roster.common_team_roster.get_dict().get("data", [])
        except Exception as exc:
            print(f"⚠️  No se pudo obtener la plantilla para team_id={nba_team_id}: {exc}", file=sys.stderr)
            continue
        for row in data:
            try:
                player_id = int(row[14])  # PLAYER_ID column per headers
            except (ValueError, TypeError):
                continue
            position = row[7]  # POSITION column
            if position:
                metadata[player_id] = (position, internal_team_id)
            elif player_id not in metadata:
                metadata[player_id] = ("", internal_team_id)
    return metadata


def upsert_players(conn, team_map: Dict[int, int]) -> Tuple[int, int]:
    """Insert or update active players. Returns (active_count, total_considered)."""
    payload = nba_players_static.get_players()
    if not payload:
        print("⚠️  nba_api.get_players() devolvió una lista vacía", file=sys.stderr)
        return (0, 0)

    player_metadata = _fetch_player_metadata(team_map)

    active_records = []
    for entry in payload:
        if not entry.get("is_active"):
            continue
        nba_player_id = entry.get("id")
        if nba_player_id is None:
            continue
        full_name = entry.get("full_name") or ""
        meta = player_metadata.get(int(nba_player_id))
        roster_position = meta[0] if meta else ""
        internal_team_id = meta[1] if meta else None
        position = roster_position or entry.get("position") or ""
        active_records.append(
            (
                nba_player_id,
                full_name,
                internal_team_id,
                position,
            )
        )

    if not active_records:
        print("⚠️  No se encontraron jugadores activos en la respuesta de nba_api", file=sys.stderr)
        return (0, 0)

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO nba_players (nba_player_id, full_name, team_id, position)
            VALUES %s
            ON CONFLICT (nba_player_id) DO UPDATE
            SET full_name = EXCLUDED.full_name,
                team_id = EXCLUDED.team_id,
                position = EXCLUDED.position
            """,
            active_records,
        )
    conn.commit()
    print(f"✅ Sincronizados {len(active_records)} jugadores activos")
    return (len(active_records), len(payload))


def main() -> int:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL no está definido. Carga tus variables o revisa el .env.", file=sys.stderr)
        return 1

    print("⏳ Conectando a la base de datos...")
    with psycopg2.connect(database_url) as conn:
        conn.autocommit = False
        team_map = upsert_teams(conn)
        upsert_players(conn, team_map)
    print("🎉 Importación completada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
