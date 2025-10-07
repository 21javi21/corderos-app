import os
import base64
import hashlib
from ldap3 import Server, Connection, ALL, SUBTREE, MODIFY_ADD, MODIFY_DELETE, MODIFY_REPLACE
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.security import (
    SessionUser,
    clear_session,
    establish_session,
    optional_user,
    require_admin,
    require_user,
)
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")
router = APIRouter(prefix="/auth", tags=["auth"])

LDAP_URI = os.getenv("LDAP_URI")
LDAP_BASE_DN = os.getenv("LDAP_BASE_DN")
LDAP_BIND_DN = os.getenv("LDAP_BIND_DN")
LDAP_BIND_PASSWORD = os.getenv("LDAP_BIND_PASSWORD")
LDAP_GROUP_DN = os.getenv("LDAP_GROUP_DN")


def fetch_all_user_uids() -> list[str]:
    """Return a sorted list of LDAP user identifiers.

    This helper is imported by other modules (e.g. for dropdowns) so we keep
    the dependency here to avoid repeating the LDAP connection boilerplate.
    Failures are swallowed and reported as an empty list to keep the UI usable
    even if LDAP is momentarily unavailable.
    """

    required_settings = [LDAP_URI, LDAP_BASE_DN, LDAP_BIND_DN, LDAP_BIND_PASSWORD]
    if not all(required_settings):
        return []

    server = Server(LDAP_URI, get_info=ALL)
    user_ids: list[str] = []

    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            conn.search(
                search_base=LDAP_BASE_DN,
                search_filter="(objectClass=inetOrgPerson)",
                search_scope=SUBTREE,
                attributes=["uid"],
            )
            for entry in conn.entries:
                if "uid" in entry and str(entry.uid):
                    user_ids.append(str(entry.uid))
    except Exception as exc:  # pragma: no cover - defensive logging for ops
        print(f"⚠️  Unable to fetch LDAP users: {exc}")

    return sorted(set(user_ids))

@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, current_user: SessionUser = Depends(require_user)):
    # Use role-specific layout
    template_name = "layout_admin.html" if current_user["is_admin"] else "layout_user.html"
    
    return templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "is_admin": current_user["is_admin"],
        },
    )

def make_ssha(password: str) -> str:
    """Genera un hash {SSHA} compatible con slappasswd."""
    salt = os.urandom(4)
    sha = hashlib.sha1(password.encode("utf-8"))
    sha.update(salt)
    return "{SSHA}" + base64.b64encode(sha.digest() + salt).decode("utf-8")


def ldap_authenticate(username: str, password: str) -> bool:
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            conn.search(
                search_base=LDAP_BASE_DN,
                search_filter=f"(|(uid={username})(cn={username}))",
                search_scope=SUBTREE,
                attributes=["cn", "uid", "mail"],
            )
            print(f"Entries: {conn.entries}")
            if not conn.entries:
                return False
            user_dn = conn.entries[0].entry_dn
            print(f"Found DN: {user_dn}")

        with Connection(server, user_dn, password, auto_bind=True):
            print("✅ User bind successful")
            return True
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def is_admin(conn, user_dn: str) -> bool:
    # Busca si user_dn pertenece al grupo admins
    conn.search(
        search_base="ou=Groups,dc=kaligulix,dc=com",
        search_filter=f"(&(objectClass=groupOfNames)(cn=admins)(member={user_dn}))",
        attributes=["cn"]
    )
    return len(conn.entries) > 0

@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request, current_user: SessionUser | None = Depends(optional_user)):
    if current_user:
        target = "/auth/dashboard_admin" if current_user["is_admin"] else "/auth/dashboard_user"
        return RedirectResponse(url=target)
    return templates.TemplateResponse("login.html", {"request": request})

@router.post("/login", response_class=HTMLResponse)
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            conn.search(
                search_base=LDAP_BASE_DN,
                search_filter=f"(uid={username})",
                search_scope=SUBTREE,
                attributes=["cn", "sn", "mail", "uid"],
            )
            if not conn.entries:
                raise HTTPException(status_code=401, detail="Invalid user")

            entry = conn.entries[0]
            user_dn = entry.entry_dn
            uid_value = str(entry.uid) if "uid" in entry and str(entry.uid) else username

        with Connection(server, user_dn, password, auto_bind=True):
            with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as check_conn:
                is_user_admin = is_admin(check_conn, user_dn)

        clear_session(request)
        establish_session(request, uid=uid_value, is_admin=is_user_admin)
        redirect_target = "/auth/dashboard_admin" if is_user_admin else "/auth/dashboard_user"
        return RedirectResponse(url=redirect_target, status_code=303)

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Exception: {e}")
        raise HTTPException(status_code=401, detail="Invalid credentials")


def _logout_response(request: Request) -> RedirectResponse:
    clear_session(request)
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@router.post("/logout")
def logout_post(request: Request) -> RedirectResponse:
    return _logout_response(request)

@router.post("/add_user")
def add_user(
    username: str = Form(...),
    password: str = Form(...),
    cn: str = Form(...),
    sn: str = Form(...),
    mail: str = Form(...),
    current_admin: SessionUser = Depends(require_admin),
):
    _ = current_admin  # dependency enforces autorización
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            dn = f"uid={username},ou=Users,{LDAP_BASE_DN}"

            # Generar password hash (SSHA)
            hashed_pw = make_ssha(password)

            attrs = {
                "objectClass": ["inetOrgPerson", "posixAccount"],
                "cn": cn,
                "sn": sn,
                "uid": username,
                "mail": mail,
                "uidNumber": "2001",  # ⚠️ hardcoded por ahora
                "gidNumber": "2001",
                "homeDirectory": f"/home/{username}",
                "loginShell": "/bin/bash",
                "userPassword": hashed_pw,
            }
            if conn.add(dn, attributes=attrs):
                # Añadir al grupo "users"
                group_dn = f"cn=users,ou=Groups,{LDAP_BASE_DN}"
                conn.modify(
                    group_dn,
                    {"member": [(MODIFY_ADD, [dn])]}
                )
                return {"message": f"✅ User {username} created and added to 'users' group!"}
            else:
                return {"error": conn.result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/panel", response_class=HTMLResponse)
def show_panel(request: Request, current_user: SessionUser = Depends(require_user)):
    return templates.TemplateResponse(
        "user_panel.html",
        {
            "request": request,
            "is_admin": current_user["is_admin"],
        },
    )

@router.get("/add_user_form", response_class=HTMLResponse)
def add_user_form(request: Request, current_admin: SessionUser = Depends(require_admin)):
    return templates.TemplateResponse(
        "add_user_form.html",
        {
            "request": request,
            "is_admin": current_admin["is_admin"],
        },
    )

@router.get("/list_users", response_class=HTMLResponse)
def list_users(request: Request, current_admin: SessionUser = Depends(require_admin)):
    server = Server(LDAP_URI, get_info=ALL)
    users = []
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            conn.search(
                search_base=LDAP_BASE_DN,
                search_filter="(objectClass=inetOrgPerson)",
                search_scope=SUBTREE,
                attributes=["uid", "cn", "sn", "mail"]
            )
            entries = list(conn.entries)
            for entry in entries:
                uid = str(entry.uid) if "uid" in entry else ""
                if not uid:
                    continue

                user_dn = _user_dn(uid)
                groups = _fetch_user_groups(conn, user_dn)
                group_label = "admins" if "admins" in groups else "users"

                users.append({
                    "uid": uid,
                    "cn": str(entry.cn) if "cn" in entry else "",
                    "sn": str(entry.sn) if "sn" in entry else "",
                    "mail": str(entry.mail) if "mail" in entry else "",
                    "group": group_label,
                })
            users.sort(key=lambda item: item["uid"].lower())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return templates.TemplateResponse(
        "list_users.html",
        {
            "request": request,
            "users": users,
            "is_admin": current_admin["is_admin"],
        },
    )


@router.get("/edit_user/{username}", response_class=HTMLResponse)
def edit_user_form(
    request: Request,
    username: str,
    current_admin: SessionUser = Depends(require_admin),
):
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            user_dn = _user_dn(username)
            conn.search(
                search_base=user_dn,
                search_filter="(objectClass=inetOrgPerson)",
                attributes=["uid", "cn", "sn", "mail"]
            )
            if not conn.entries:
                raise HTTPException(status_code=404, detail="User not found")

            entry = conn.entries[0]
            groups = _fetch_user_groups(conn, user_dn)
            group_label = "admins" if "admins" in groups else "users"

            user = {
                "uid": username,
                "cn": str(entry.cn) if "cn" in entry else "",
                "sn": str(entry.sn) if "sn" in entry else "",
                "mail": str(entry.mail) if "mail" in entry else "",
                "group": group_label,
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return templates.TemplateResponse(
        "edit_user_form.html",
        {
            "request": request,
            "user": user,
            "is_admin": current_admin["is_admin"],
        },
    )


@router.post("/edit_user/{username}")
def edit_user(
    username: str,
    cn: str = Form(...),
    sn: str = Form(...),
    mail: str = Form(...),
    current_admin: SessionUser = Depends(require_admin),
):
    _ = current_admin
    server = Server(LDAP_URI, get_info=ALL)
    user_dn = _user_dn(username)

    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            modifications = {
                "cn": [(MODIFY_REPLACE, [cn])],
                "sn": [(MODIFY_REPLACE, [sn])],
                "mail": [(MODIFY_REPLACE, [mail])],
            }
            if not conn.modify(user_dn, modifications):
                raise HTTPException(status_code=500, detail=str(conn.result))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return RedirectResponse(url="/auth/list_users", status_code=303)

@router.post("/delete_user")
def delete_user(username: str = Form(...), current_admin: SessionUser = Depends(require_admin)):
    _ = current_admin
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            dn = f"uid={username},ou=Users,{LDAP_BASE_DN}"
            if conn.delete(dn):
                return RedirectResponse(url="/auth/list_users", status_code=303)
            else:
                return {"error": conn.result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/change_group")
def change_group(
    username: str = Form(...),
    group: str = Form(...),
    current_admin: SessionUser = Depends(require_admin),
):
    _ = current_admin
    server = Server(LDAP_URI, get_info=ALL)
    user_dn = _user_dn(username)

    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            current_groups = _fetch_user_groups(conn, user_dn)
            for grp in current_groups:
                if not conn.modify(
                    f"cn={grp},{_groups_base_dn()}",
                    {"member": [(MODIFY_DELETE, [user_dn])]}
                ):
                    if conn.result.get("description") not in {"success", "noSuchAttribute"}:
                        raise HTTPException(status_code=500, detail=str(conn.result))
            # Añadir al grupo elegido
            if not conn.modify(
                f"cn={group},{_groups_base_dn()}",
                {"member": [(MODIFY_ADD, [user_dn])]}
            ):
                raise HTTPException(status_code=500, detail=str(conn.result))
            return RedirectResponse(url="/auth/list_users", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard_admin", response_class=HTMLResponse)
def dashboard_admin(request: Request, current_admin: SessionUser = Depends(require_admin)):
    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "is_admin": current_admin["is_admin"],
        },
    )

@router.get("/dashboard_user", response_class=HTMLResponse)
def dashboard_user(request: Request, current_user: SessionUser = Depends(require_user)):
    return templates.TemplateResponse(
        "user_dashboard.html",
        {
            "request": request,
            "is_admin": current_user["is_admin"],
        },
    )


def _user_dn(username: str) -> str:
    return f"uid={username},ou=Users,{LDAP_BASE_DN}"


def _groups_base_dn() -> str:
    return f"ou=Groups,{LDAP_BASE_DN}"


def _fetch_user_groups(conn: Connection, user_dn: str) -> set[str]:
    groups: set[str] = set()
    conn.search(
        search_base=_groups_base_dn(),
        search_filter=f"(&(objectClass=groupOfNames)(member={user_dn}))",
        attributes=["cn"],
    )
    for entry in conn.entries:
        if "cn" in entry:
            groups.add(str(entry.cn).lower())
    return groups
