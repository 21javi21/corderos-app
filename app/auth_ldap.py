import os
import base64
import hashlib
from ldap3 import Server, Connection, ALL, SUBTREE
from fastapi import Depends, HTTPException, Form, APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")
router = APIRouter(prefix="/auth", tags=["auth"])

LDAP_URI = os.getenv("LDAP_URI")
LDAP_BASE_DN = os.getenv("LDAP_BASE_DN")
LDAP_BIND_DN = os.getenv("LDAP_BIND_DN")
LDAP_BIND_PASSWORD = os.getenv("LDAP_BIND_PASSWORD")
LDAP_GROUP_DN = os.getenv("LDAP_GROUP_DN")


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
    # Chequear si user_dn está en grupo admins
    conn.search(
        search_base=LDAP_GROUP_DN,
        search_filter=f"(member={user_dn})",
        attributes=["cn"]
    )
    return len(conn.entries) > 0

@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@router.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            # Buscar DN del usuario
            conn.search(
                search_base=LDAP_BASE_DN,
                search_filter=f"(uid={username})",
                attributes=["dn"]
            )
            if not conn.entries:
                raise HTTPException(status_code=401, detail="Invalid user")
            user_dn = conn.entries[0].entry_dn

        # Probar login real
        with Connection(server, user_dn, password, auto_bind=True):
            # Bind correcto → ahora verificar si es admin
            with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as check_conn:
                if is_admin(check_conn, user_dn):
                    return templates.TemplateResponse("admin_dashboard.html", {"request": {}, "username": username})
                else:
                    return templates.TemplateResponse("user_dashboard.html", {"request": {}, "username": username})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid credentials")

@router.post("/add_user")
def add_user(
    username: str = Form(...),
    password: str = Form(...),
    cn: str = Form(...),
    sn: str = Form(...),
    mail: str = Form(...),
):
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
                return {"message": f"✅ User {username} created successfully!"}
            else:
                return {"error": conn.result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/panel", response_class=HTMLResponse)
def show_panel(request: Request):
    return templates.TemplateResponse("user_panel.html", {"request": request})

@router.get("/add_user_form", response_class=HTMLResponse)
def add_user_form(request: Request):
    return templates.TemplateResponse("add_user_form.html", {"request": request})

@router.get("/list_users", response_class=HTMLResponse)
def list_users(request: Request):
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
            for entry in conn.entries:
                users.append({
                    "uid": str(entry.uid) if "uid" in entry else "",
                    "cn": str(entry.cn) if "cn" in entry else "",
                    "sn": str(entry.sn) if "sn" in entry else "",
                    "mail": str(entry.mail) if "mail" in entry else "",
                })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return templates.TemplateResponse("list_users.html", {"request": request, "users": users})

@router.post("/delete_user")
def delete_user(username: str = Form(...)):
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