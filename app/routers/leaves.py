from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from ..schemas import LeaveCreate, LeaveOut
from ..models import Leave, Employee, User
# --- MODIFIÉ ---
from ..auth import api_require_permission
from ..deps import get_db, api_current_user # Renommé
# --- FIN MODIFIÉ ---

router = APIRouter(prefix="/api/leaves", tags=["leaves"])

# --- MODIFIÉ : Utilise la nouvelle dépendance de permission ---
@router.post("/", response_model=LeaveOut, dependencies=[Depends(api_require_permission("can_manage_leaves"))])
# --- FIN MODIFIÉ ---
async def create_leave(
    payload: LeaveCreate, 
    db: AsyncSession = Depends(get_db), 
    user: User = Depends(api_current_user) # Renommé
):
    """Create a new leave request."""
    # Validation
    res = await db.execute(select(Employee).where(Employee.id == payload.employee_id))
    employee = res.scalar_one_or_none()
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")

    # --- MODIFIÉ : Vérification de permission par branche ---
    if not user.permissions.is_admin and user.branch_id != employee.branch_id:
        raise HTTPException(status_code=403, detail="Not authorized for this branch")
    # --- FIN MODIFIÉ ---

    leave = Leave(
        **payload.model_dump(),
        created_by=user.id
    )
    db.add(leave)
    await db.commit()
    await db.refresh(leave)
    return leave


@router.get("/", response_model=List[LeaveOut])
async def list_leaves(db: AsyncSession = Depends(get_db)):
    """List all leave requests."""
    res = await db.execute(select(Leave))
    return res.scalars().all()
