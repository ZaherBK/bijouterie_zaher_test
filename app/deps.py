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


from sqlalchemy import select
from sqlalchemy.orm import selectinload

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
    async def dep(
        request: Request,
        user_sess = Depends(get_current_session_user),
        db: AsyncSession = Depends(get_db)
    ):
        if isinstance(user_sess, RedirectResponse):
            raise HTTPException(status_code=status.HTTP_302_FOUND,
                                detail="Not authenticated",
                                headers={"Location": user_sess.headers["location"]})
        
        # --- FIX: Reload permissions from DB ---
        user_id = user_sess.get("id")
        if user_id:
            res = await db.execute(select(User).options(selectinload(User.permissions)).where(User.id == user_id))
            fresh_user = res.scalar_one_or_none()
            if fresh_user and fresh_user.is_active:
                 if fresh_user.permissions:
                     user_sess["permissions"] = fresh_user.permissions.to_dict()
                 else:
                     user_sess["permissions"] = {}
        # --------------------------------------

        perms = user_sess.get("permissions", {})
        if perms.get("is_admin"):
            return user_sess
        if permission == "is_admin":
            raise HTTPException(status_code=status.HTTP_302_FOUND,
                                detail="Insufficient permissions (Admin required)",
                                headers={"Location": str(request.url_for('home'))})
        if not perms.get(permission):
            raise HTTPException(status_code=status.HTTP_302_FOUND,
                                detail="Insufficient permissions",
                                headers={"Location": str(request.url_for('home'))})
        return user_sess
    return dep
