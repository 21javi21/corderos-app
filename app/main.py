from fastapi import FastAPI
from app import auth_ldap   # make sure app/auth_ldap.py exists
from fastapi.responses import HTMLResponse
from fastapi import APIRouter

app = FastAPI()
router = APIRouter()
app.include_router(auth_ldap.router)

@app.get("/")
def read_root():
    return {"message": "Corderos App is alive 🎉"}

@router.get("/login", response_class=HTMLResponse)
def login_page():
    return """
    <html>
      <body>
        <form action="/auth/login" method="post">
          <label>Username:</label>
          <input type="text" name="username">
          <br>
          <label>Password:</label>
          <input type="password" name="password">
          <br>
          <input type="submit" value="Login">
        </form>
      </body>
    </html>
    """