"""
User-related API endpoints.

Exposes endpoints for user login and user creation. The login endpoint
delegates to the authentication module, while user creation requires
administrative privileges.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..schemas import UserCreate, UserOut
from ..models import User
# --- MODIFIÉ ---
from ..auth import login, hash_password, api_require_permission
# --- FIN MODIFIÉ ---
from ..deps import get_db

router = APIRouter(prefix="/api/users", tags=["users"])


@router.post("/login")
async def login_proxy(token=Depends(login)):
    """Proxy for login to return a token."""
    return token


# --- MODIFIÉ : Utilise la nouvelle dépendance de permission ---
@router.post("/", response_model=UserOut, dependencies=[Depends(api_require_permission("can_manage_users"))])
# --- FIN MODIFIÉ ---
async def create_user(payload: UserCreate, db: AsyncSession = Depends(get_db)):
    """Create a new user. Only admins may call this endpoint."""
    exists = await db.execute(select(User).where(User.email == payload.email))
    if exists.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already exists")
    
    # --- MODIFIÉ : Utilise role_id ---
    user = User(
        email=payload.email,
        full_name=payload.full_name,
        role_id=payload.role_id, # Changé de 'role'
        branch_id=payload.branch_id,
        hashed_password=hash_password(payload.password),
    )
    # --- FIN MODIFIÉ ---
    
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user
