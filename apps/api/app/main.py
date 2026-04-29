from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select

from .config import settings
from .db import Base, SessionLocal, engine
from .models import User, UserRole
from .routers.auth import router as auth_router
from .routers.users import router as users_router
from .security import hash_password

app = FastAPI(title="VigSocial API", version="0.1.0")


def _cors_allow_origins() -> list[str]:
    raw = settings.cors_origins.strip()
    if raw == "*":
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]


app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins(),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def bootstrap_superadmin() -> None:
    if not settings.bootstrap_superadmin_email or not settings.bootstrap_superadmin_password:
        return

    with SessionLocal() as db:
        existing = db.scalar(select(User).where(User.email == settings.bootstrap_superadmin_email))
        if existing:
            return

        user = User(
            name=settings.bootstrap_superadmin_name,
            email=settings.bootstrap_superadmin_email,
            password_hash=hash_password(settings.bootstrap_superadmin_password),
            role=UserRole.SUPERADMIN,
        )
        db.add(user)
        db.commit()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
    bootstrap_superadmin()


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(auth_router, prefix="/api/v1")
app.include_router(users_router, prefix="/api/v1")
