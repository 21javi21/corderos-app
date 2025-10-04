from pydantic import BaseSettings
import os

class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    secret_key: str = os.getenv("SECRET_KEY", "your-secret-key")
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    ldap_server: str = os.getenv("LDAP_SERVER", "")
    ldap_base_dn: str = os.getenv("LDAP_BASE_DN", "")
    
    class Config:
        env_file = ".env"

settings = Settings()
