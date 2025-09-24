from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app import auth_ldap

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# include auth routes
app.include_router(auth_ldap.router)

@app.get("/", response_class=HTMLResponse)
def root_redirect():
    # always send users to the login page
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    # render the login form
    return templates.TemplateResponse("login.html", {"request": request})