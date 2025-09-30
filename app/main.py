import os
import re
import secrets
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path as PathlibPath

from fastapi import Depends, FastAPI, File, Form, HTTPException, Path, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
import psycopg2
from psycopg2 import errors
from psycopg2.pool import SimpleConnectionPool

from app import auth_ldap
from app.security import SessionUser, optional_user, require_admin, require_user

CATEGORIAS_PREDEFINIDAS = [
    "Futbol",
    "Basket",
    "Tennis",
    "Politica",
    "Otros",
]

MULTIPLICA_OPCIONES = [1, 2, 3, 4, 5]

AUTO_LOCK_DAYS = 3

HALL_OF_HATE_NAMES = [
    "Lebron James",
    "Luka Doncic",
    "Carlo Ancelotti",
    "\"El Cholo\" Simeone",
    "Vinicius",
    "Xabi Alonso",
]

HALL_OF_HATE_DIR = PathlibPath("app/images/hall_of_hate")
_HALL_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


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

app = FastAPI()
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
app.mount("/static", StaticFiles(directory="app/images"), name="static")

DATABASE_URL = os.environ.get("DATABASE_URL")

pool: SimpleConnectionPool | None = None


def _ensure_schema(conn) -> None:
    with conn, conn.cursor() as cur:
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


def _parse_locked_value(value: str | None, current: bool) -> bool:
    if value is None:
        return current
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "locked", "bloqueada", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "unlocked", "desbloqueada", "off"}:
        return False
    return current


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


def _fetch_hall_of_hate_db_entries() -> list[dict[str, str | None]]:
    if not pool:
        return []
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, image_filename
                FROM hall_of_hate
                ORDER BY created_at DESC, id DESC
                """
            )
            rows = cur.fetchall()
    finally:
        pool.putconn(conn)

    entries: list[dict[str, str | None]] = []
    for name, image_filename in rows:
        image_path = f"hall_of_hate/{image_filename}" if image_filename else None
        entries.append({
            "name": name,
            "image": image_path,
        })
    return entries


def _insert_hall_of_hate_entry(name: str, image_filename: str) -> None:
    if not pool:
        raise HTTPException(status_code=500, detail="Conexión a base de datos no inicializada")
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hall_of_hate (name, image_filename)
                VALUES (%s, %s)
                """,
                (name, image_filename),
            )
    finally:
        pool.putconn(conn)


def _save_hall_of_hate_image(upload: UploadFile, display_name: str) -> str:
    content_type = (upload.content_type or "").lower()
    if content_type != "image/png":
        raise HTTPException(status_code=400, detail="Solo se permiten imágenes PNG")

    data = upload.file.read()
    if not data:
        raise HTTPException(status_code=400, detail="El archivo está vacío")
    if not data.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=400, detail="El archivo no es un PNG válido")

    HALL_OF_HATE_DIR.mkdir(parents=True, exist_ok=True)
    base_slug = _slugify(display_name) or f"entry_{secrets.token_hex(2)}"
    filename = f"{base_slug}_{secrets.token_hex(4)}.png"
    destination = HALL_OF_HATE_DIR / filename
    with destination.open("wb") as out_file:
        out_file.write(data)
    upload.file.close()
    return filename

def _hall_of_hate_entries() -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    seen_names: set[str] = set()

    db_entries = _fetch_hall_of_hate_db_entries()
    for item in db_entries:
        entries.append(item)
        seen_names.add(item["name"].strip().lower())

    files_by_slug: dict[str, str] = {}
    if HALL_OF_HATE_DIR.exists():
        for image_path in HALL_OF_HATE_DIR.iterdir():
            if not image_path.is_file():
                continue
            files_by_slug[_slugify(image_path.stem)] = image_path.name

    for name in HALL_OF_HATE_NAMES:
        normalized = name.strip().lower()
        if normalized in seen_names:
            continue
        slug = _slugify(name)
        filename = files_by_slug.get(slug)
        entries.append({
            "name": name,
            "image": f"hall_of_hate/{filename}" if filename else None,
        })

    return entries

@app.on_event("startup")
def startup_db():
    global pool
    if not DATABASE_URL:
        # Usa el ConfigMap ya desplegado en K8s; localmente puedes exportar DATABASE_URL
        raise RuntimeError("DATABASE_URL no está definido")
    pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
    HALL_OF_HATE_DIR.mkdir(parents=True, exist_ok=True)
    conn = pool.getconn()
    try:
        _ensure_schema(conn)
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


@app.get("/hall-of-hate", response_class=HTMLResponse)
def hall_of_hate(request: Request, current_user: SessionUser | None = Depends(optional_user)):
    is_admin = bool(current_user and current_user["is_admin"])
    return templates.TemplateResponse(
        "hall_of_hate.html",
        {
            "request": request,
            "entries": _hall_of_hate_entries(),
            "is_admin": is_admin,
        }
    )


@app.get("/hall-of-hate/nuevo", response_class=HTMLResponse)
def hall_of_hate_new(request: Request, current_admin: SessionUser = Depends(require_admin)):
    return templates.TemplateResponse(
        "hall_of_hate_new.html",
        {
            "request": request,
            "is_admin": current_admin["is_admin"],
        }
    )


@app.post("/hall-of-hate/nuevo")
def hall_of_hate_create(
    request: Request,
    nombre: str = Form(...),
    imagen: UploadFile = File(...),
    current_admin: SessionUser = Depends(require_admin),
):
    clean_name = nombre.strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="El nombre es obligatorio")

    filename = _save_hall_of_hate_image(imagen, clean_name)
    try:
        _insert_hall_of_hate_entry(clean_name, filename)
    except psycopg2.Error as exc:
        # Limpia el archivo recién creado si la inserción falla
        file_path = HALL_OF_HATE_DIR / filename
        if file_path.exists():
            file_path.unlink(missing_ok=True)
        if isinstance(exc, errors.UniqueViolation):
            raise HTTPException(status_code=400, detail="Ya existe un villano con ese nombre") from exc
        raise HTTPException(status_code=500, detail="No se pudo guardar el registro") from exc

    return RedirectResponse(url="/hall-of-hate", status_code=303)


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
