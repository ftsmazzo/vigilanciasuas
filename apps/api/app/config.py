from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = "postgresql+psycopg://vigsocial:vigsocial_dev@localhost:5432/vigsocial"
    redis_url: str = "redis://localhost:6379/0"
    jwt_secret_key: str = "change_me"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60

    bootstrap_superadmin_email: str | None = None
    bootstrap_superadmin_password: str | None = None
    bootstrap_superadmin_name: str = "Super Admin"


settings = Settings()
