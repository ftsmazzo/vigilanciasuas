from pydantic import BaseModel, EmailStr

from .models import UserRole


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: UserRole
    name: str


class UserCreateRequest(BaseModel):
    name: str
    email: EmailStr
    password: str
    role: UserRole


class UserOut(BaseModel):
    id: int
    name: str
    email: EmailStr
    role: UserRole

    class Config:
        from_attributes = True
