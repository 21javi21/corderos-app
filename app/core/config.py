from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    secret_key: str = os.getenv("SECRET_KEY", "your-secret-key")
    session_secret: str = os.getenv("SESSION_SECRET", "change-me-in-production")
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    ldap_uri: str = os.getenv("LDAP_URI", "")
    ldap_base_dn: str = os.getenv("LDAP_BASE_DN", "")
    ldap_bind_dn: str = os.getenv("LDAP_BIND_DN", "")
    ldap_bind_password: str = os.getenv("LDAP_BIND_PASSWORD", "")
    ldap_group_dn: str = os.getenv("LDAP_GROUP_DN", "")
    
    class Config:
        env_file = ".env"

settings = Settings()
