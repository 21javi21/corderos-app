import os
from datetime import date
from fastapi import FastAPI, Request, Query, Form, Path, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from app import auth_ldap

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.include_router(auth_ldap.router)

DATABASE_URL = os.environ.get("DATABASE_URL")

pool: SimpleConnectionPool | None = None

@app.on_event("startup")
def startup_db():
    global pool
    if not DATABASE_URL:
        # Usa el ConfigMap ya desplegado en K8s; localmente puedes exportar DATABASE_URL
        raise RuntimeError("DATABASE_URL no está definido")
    pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)

@app.on_event("shutdown")
def shutdown_db():
    global pool
    if pool:
        pool.closeall()
        pool = None

@app.get("/", response_class=HTMLResponse)
def root_redirect():
    # always send users to the login page
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    # render the login form
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/user_dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("user_dashboard.html", {"request": request})

@app.get("/bets", response_class=HTMLResponse)
def bets_home(request: Request, page: int = Query(1, ge=1)):
    limit = 25
    offset = (page - 1) * limit

    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, apuesta, creacion, categoria, tipo, multiplica,
                       apostante1, apostante2, apostante3,
                       apostado1, apostado2, apostado3,
                       ganador1, ganador2, perdedor1, perdedor2
                FROM apuestas
                ORDER BY id DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            rows = cur.fetchall()

            # para saber si hay siguiente página
            cur.execute("SELECT COUNT(*) FROM apuestas")
            total = cur.fetchone()[0]
    finally:
        pool.putconn(conn)

    apuestas = [
        {
            "id": r[0], "apuesta": r[1], "creacion": r[2], "categoria": r[3], "tipo": r[4],
            "multiplica": r[5],
            "apostante1": r[6], "apostante2": r[7], "apostante3": r[8],
            "apostado1": r[9], "apostado2": r[10], "apostado3": r[11],
            "ganador1": r[12], "ganador2": r[13], "perdedor1": r[14], "perdedor2": r[15],
        } for r in rows
    ]

    has_prev = page > 1
    has_next = (page * limit) < total

    return templates.TemplateResponse(
        "bets.html",
        {"request": request, "apuestas": apuestas, "page": page, "has_prev": has_prev, "has_next": has_next}
    )


@app.get("/apuestas/nueva", response_class=HTMLResponse)
def nueva_apuesta_form(request: Request):
    return templates.TemplateResponse("add_apuesta.html", {"request": request})


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
):
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO apuestas (
                    apuesta, creacion, categoria, tipo, multiplica,
                    apostante1, apostante2, apostante3,
                    apostado1, apostado2, apostado3,
                    ganador1, ganador2, perdedor1, perdedor2
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s
                ) RETURNING id
            """, (
                apuesta, date.today(), categoria, tipo, multiplica,
                apostante1, apostante2, apostante3,
                apostado1, apostado2, apostado3,
                ganador1, ganador2, perdedor1, perdedor2
            ))
            _new_id = cur.fetchone()[0]
    finally:
        pool.putconn(conn)

    return RedirectResponse(url="/bets", status_code=303)

@app.post("/apuestas/{apuesta_id}/borrar")
def borrar_apuesta(apuesta_id: int = Path(...)):
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM apuestas WHERE id = %s", (apuesta_id,))
    finally:
        pool.putconn(conn)
    return RedirectResponse(url="/bets", status_code=303)


@app.get("/apuestas/{apuesta_id}/editar", response_class=HTMLResponse)
def editar_apuesta_form(request: Request, apuesta_id: int = Path(...)):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, apuesta, categoria, tipo, multiplica,
                       apostante1, apostante2, apostante3,
                       apostado1, apostado2, apostado3,
                       ganador1, ganador2, perdedor1, perdedor2
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
    }

    return templates.TemplateResponse(
        "edit_apuesta.html",
        {"request": request, "apuesta": apuesta},
    )


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


@app.post("/apuestas/{apuesta_id}/editar")
def actualizar_apuesta(
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
):
    conn = pool.getconn()
    try:
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
                    perdedor2 = %s
                WHERE id = %s
                """,
                (
                    apuesta,
                    categoria,
                    tipo,
                    multiplica,
                    _empty_to_none(apostante1),
                    _empty_to_none(apostante2),
                    _empty_to_none(apostante3),
                    _empty_to_none(apostado1),
                    _empty_to_none(apostado2),
                    _empty_to_none(apostado3),
                    _empty_to_none(ganador1),
                    _empty_to_none(ganador2),
                    _empty_to_none(perdedor1),
                    _empty_to_none(perdedor2),
                    apuesta_id,
                ),
            )
    finally:
        pool.putconn(conn)

    return RedirectResponse(url="/bets", status_code=303)
