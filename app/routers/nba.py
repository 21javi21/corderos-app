# app/routers/nba.py
import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.concurrency import run_in_threadpool

from app.services.nba_stats import get_team_advanced, get_mvp_ladder, get_roy_ladder

router = APIRouter(prefix="/nba", tags=["nba"])
templates = Jinja2Templates(directory="app/templates")

@router.get("/tracker", response_class=HTMLResponse)
async def tracker(request: Request):
    team_adv, mvp, roy = await asyncio.gather(
        run_in_threadpool(get_team_advanced),
        run_in_threadpool(get_mvp_ladder),
        run_in_threadpool(get_roy_ladder),
    )
    return templates.TemplateResponse(
        "nba_tracker.html",
        {
            "request": request,
            "team_adv": team_adv,
            "mvp": mvp,
            "roy": roy,
            "season": "2025-26",
        },
    )
