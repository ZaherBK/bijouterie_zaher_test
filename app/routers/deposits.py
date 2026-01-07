from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from ..schemas import DepositCreate, DepositOut
from ..models import Deposit, Employee, User
# --- MODIFIÉ ---
from ..auth import api_require_permission
from ..deps import get_db, api_current_user # Renommé
# --- FIN MODIFIÉ ---

router = APIRouter(prefix="/api/deposits", tags=["deposits"])

# --- MODIFIÉ : Utilise la nouvelle dépendance de permission ---
@router.post("/", response_model=DepositOut, dependencies=[Depends(api_require_permission("can_manage_deposits"))])
# --- FIN MODIFIÉ ---
async def create_deposit(
    payload: DepositCreate, 
    db: AsyncSession = Depends(get_db), 
    user: User = Depends(api_current_user) # Renommé
):
    """Create a new deposit (advance)."""
    # Validation
    res = await db.execute(select(Employee).where(Employee.id == payload.employee_id))
    employee = res.scalar_one_or_none()
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")

    # --- MODIFIÉ : Vérification de permission par branche ---
    if not user.permissions.is_admin and user.branch_id != employee.branch_id:
        raise HTTPException(status_code=403, detail="Not authorized for this branch")
    # --- FIN MODIFIÉ ---

    deposit = Deposit(
        **payload.model_dump(),
        created_by=user.id
    )
    db.add(deposit)
    await db.commit()
    await db.refresh(deposit)
    return deposit


@router.get("/", response_model=List[DepositOut])
async def list_deposits(db: AsyncSession = Depends(get_db)):
    """List all deposits."""
    res = await db.execute(select(Deposit).options(selectinload(Deposit.employee), selectinload(Deposit.creator)).order_by(Deposit.date.desc()))
    return res.scalars().all()


# --- NEW: Sync Endpoint ---
from pydantic import BaseModel
from datetime import date
from sqlalchemy.orm import selectinload
from ..services.legacy_sync import sync_deposits_to_legacy

class SyncRequest(BaseModel):
    host: str = "localhost"
    schema_name: str
    password: str = "6165"

@router.post("/sync", dependencies=[Depends(api_require_permission("can_manage_deposits"))])
async def sync_deposits(
    payload: SyncRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user)
):
    """
    Sync today's deposits to the legacy MySQL database.
    """
    today = date.today()
    
    # Fetch today's deposits
    query = select(Deposit).options(selectinload(Deposit.employee)).where(Deposit.date == today)
    
    # Filter by branch if not admin
    if not user.permissions.is_admin:
        query = query.join(Employee).where(Employee.branch_id == user.branch_id)
        
    res = await db.execute(query)
    deposits = res.scalars().all()
    
    if not deposits:
        return {"success": False, "message": "No deposits found for today."}
        
    # Prepare data for sync service
    deposits_data = []
    for dep in deposits:
        emp_name = f"{dep.employee.first_name} {dep.employee.last_name}" if dep.employee else "Unknown"
        deposits_data.append({
            "amount": float(dep.amount),
            "date": dep.date,
            "employee_name": emp_name,
            "note": dep.note
        })
        
    # Call the sync service
    # Note: We are running this synchronously for now as pymysql is blocking.
    # In a high-load env, this should be offloaded to a background task or thread.
    result = sync_deposits_to_legacy(
        host=payload.host,
        schema=payload.schema_name,
        password=payload.password,
        deposits=deposits_data,
        user_name=user.full_name
    )
    
    return result
