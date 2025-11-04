# app/services/nba_headers.py
"""
Shared helpers to ensure every nba_api request mimics the headers expected by
https://www.nba.com/stats/.  Calling `ensure_nba_api_headers()` once per
process is enough; the defaults are applied lazily and reused afterwards.
"""

from __future__ import annotations

from typing import Mapping

try:  # nba_api >= 1.4 ships headers in a dedicated module
    from nba_api.stats.library.headers import NBA_HEADERS  # type: ignore
except ModuleNotFoundError:  # older versions (< 1.4) keep them under library.http
    from nba_api.stats.library.http import STATS_HEADERS as NBA_HEADERS  # type: ignore

from nba_api.stats.library import http as stats_http_module  # type: ignore

_BOOTSTRAPPED = False

# Headers observed from the public NBA stats site. Using a realistic user-agent
# and referer avoids the 403s triggered by the default nba_api headers.
DEFAULT_NBA_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/stats/",
    "Connection": "keep-alive",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "x-nba-stats-token": "true",
    "x-nba-stats-origin": "stats",
}

_HEADERS_TO_DROP = {"Host", "Referer", "Origin"}

def ensure_nba_api_headers() -> Mapping[str, str]:
    """
    Apply the hardened header defaults to nba_api and return the current map.
    """
    global _BOOTSTRAPPED
    if not _BOOTSTRAPPED:
        for key in _HEADERS_TO_DROP:
            NBA_HEADERS.pop(key, None)
        NBA_HEADERS.update(DEFAULT_NBA_HEADERS)
        stats_http_module.NBAStatsHTTP.base_url = "https://stats.nba.com/stats/{endpoint}"
        _BOOTSTRAPPED = True
    return dict(NBA_HEADERS)


def attach_to_session(session) -> None:
    """
    Update a `requests.Session` with the hardened defaults. The session is
    modified in-place; callers usually want to run this once per process.
    """
    ensure_nba_api_headers()
    for key in _HEADERS_TO_DROP:
        session.headers.pop(key, None)
    session.headers.update(DEFAULT_NBA_HEADERS)
