import os
from ldap3 import Server, Connection, ALL, SUBTREE
from fastapi import Depends, HTTPException, Form, APIRouter

router = APIRouter(prefix="/auth", tags=["auth"])

LDAP_URI = os.getenv("LDAP_URI")
LDAP_BASE_DN = os.getenv("LDAP_BASE_DN")
LDAP_BIND_DN = os.getenv("LDAP_BIND_DN")
LDAP_BIND_PASSWORD = os.getenv("LDAP_BIND_PASSWORD")
LDAP_GROUP_DN = os.getenv("LDAP_GROUP_DN")

def ldap_authenticate(username: str, password: str) -> bool:
    server = Server(LDAP_URI, get_info=ALL)
    try:
        with Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True) as conn:
            conn.search(
                search_base=LDAP_BASE_DN,
                search_filter=f"(|(uid={username})(cn={username}))",
                search_scope=SUBTREE,
                attributes=["cn", "uid", "mail"]  # pedir atributos válidos
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
