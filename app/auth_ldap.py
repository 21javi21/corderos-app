import os
import base64
import hashlib
from ldap3 import Server, Connection, ALL, SUBTREE
from fastapi import Depends, HTTPException, Form, APIRouter, Body

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


@router.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    if not ldap_authenticate(username, password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"message": f"Welcome {username}!"}


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
