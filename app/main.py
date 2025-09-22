from fastapi import FastAPI
from app import auth_ldap   # make sure app/auth_ldap.py exists

app = FastAPI()

app.include_router(auth_ldap.router)

@app.get("/")
def read_root():
    return {"message": "Corderos App is alive 🎉"}