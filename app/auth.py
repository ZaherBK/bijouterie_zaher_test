"""
Authentication and authorization utilities for HR Sync.
"""
import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .db import get_session
from .models import User   # علاقة المستخدم اسمها permissions (تشير إلى Role)
from .schemas import Token

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 scheme for FastAPI (لو عندك مسار آخر لواجهة الـAPI عدّله)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

# JWT configuration
SECRET_KEY = os.getenv("SECRET_KEY", "change_me")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "720"))

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, hashed_password: str) -> bool:
    return pwd_context.verify(password, hashed_password)

async def authenticate_user(session: AsyncSession, email: str, password: str) -> Optional[User]:
    """
    Return a user if authentication succeeds; otherwise, None.
    NOTE: eager-load 'permissions' (العلاقة الجديدة بدل role)
    """
    res = await session.execute(
        select(User).options(selectinload(User.permissions)).where(User.email == email)
    )
    user = res.scalar_one_or_none()
    if not user or not verify_password(password, user.hashed_password) or not user.is_active:
        return None
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
) -> Token:
    """Dependency for API login endpoint; returns a JWT token."""
    user = await authenticate_user(session, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # خزن صلاحيات الدور داخل التوكن
    permissions = user.permissions.to_dict() if getattr(user, "permissions", None) else {}
    token_data = {
        "sub": str(user.id),
        "branch_id": user.branch_id,
        "permissions": permissions,
    }
    token = create_access_token(token_data)
    return Token(access_token=token)

async def get_current_user(
    request: Request,
    token: str | None = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_session),
) -> User:
    """Retrieve the current user from a JWT token or the web session."""
    credentials_exception = HTTPException(status_code=401, detail="Invalid credentials")

    # 1) Try bearer token (API access)
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            uid = payload.get("sub")
            if uid is None:
                raise credentials_exception
        except JWTError:
            raise credentials_exception

        res = await session.execute(
            select(User).options(selectinload(User.permissions)).where(User.id == int(uid))
        )
        user = res.scalar_one_or_none()
        if not user or not user.is_active:
            raise credentials_exception
        return user

    # 2) Fallback to session cookie (web UI)
    session_user = request.session.get("user") if request.session else None
    if session_user and session_user.get("email"):
        res = await session.execute(
            select(User).options(selectinload(User.permissions)).where(User.email == session_user["email"])
        )
        user = res.scalar_one_or_none()
        if user and user.is_active:
            return user

    # Nothing worked
    raise credentials_exception

def api_require_permission(permission: str):
    """
    Dependency factory that asserts the API user has the specified permission.
    Uses 'is_admin' as god-mode.
    """
    async def dep(user: User = Depends(get_current_user)) -> User:
        perms = getattr(user, "permissions", None)
        if not perms:
            raise HTTPException(status_code=403, detail="Insufficient permissions (no role assigned)")

        if getattr(perms, "is_admin", False):
            return user  # god mode

        if not getattr(perms, permission, False):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return dep
