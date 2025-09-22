from fastapi import FastAPI, Request
from app import auth_ldap
from fastapi.responses import HTMLResponse
from fastapi import APIRouter
from fastapi.templating import Jinja2Templates

app = FastAPI()
router = APIRouter()

app.include_router(auth_ldap.router)

templates = Jinja2Templates(directory="app/templates")

@app.get("/")
def read_root():
    return {"message": "Corderos App is alive 🎉"}


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})