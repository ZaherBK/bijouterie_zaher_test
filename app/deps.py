"""
Dependency wrappers for FastAPI.

This module defines dependencies that provide database sessions and the
currently authenticated user for route handlers.
"""
from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import AsyncSessionLocal
from typing import AsyncGenerator, Optional

from .db import get_session
from .auth import get_current_user
from .models import User


# هذا هو الـdependency اللي تستعمله بكل الراوترات
async def get_db(session: AsyncSession = Depends(get_session)) -> AsyncGenerator[AsyncSession, None]:
    yield session

async def api_current_user(user: User = Depends(get_current_user)) -> User:
    return user

def get_user_data_from_session_safe(request: Request) -> Optional[dict]:
    return request.session.get("user")

def get_current_session_user(request: Request):
    user = request.session.get("user")
    if user is None:
        return RedirectResponse(request.url_for('login_page'), status_code=status.HTTP_302_FOUND)
    return user

def web_require_permission(permission: str):
    def dep(
        request: Request,
        user = Depends(get_current_session_user)
    ):
        if isinstance(user, RedirectResponse):
            raise HTTPException(status_code=status.HTTP_302_FOUND,
                                detail="Not authenticated",
                                headers={"Location": user.headers["location"]})
        perms = user.get("permissions", {})
        if perms.get("is_admin"):
            return user
        if permission == "is_admin":
            raise HTTPException(status_code=status.HTTP_302_FOUND,
                                detail="Insufficient permissions (Admin required)",
                                headers={"Location": str(request.url_for('home'))})
        if not perms.get(permission):
            raise HTTPException(status_code=status.HTTP_302_FOUND,
                                detail="Insufficient permissions",
                                headers={"Location": str(request.url_for('home'))})
        return user
    return dep
