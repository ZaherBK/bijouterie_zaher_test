from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..schemas import AttendanceCreate, AttendanceOut
from ..models import Attendance, Employee, User
# --- MODIFIÉ ---
from ..auth import api_require_permission
from ..deps import get_db, api_current_user # Renommé
# --- FIN MODIFIÉ ---

router = APIRouter(prefix="/api/attendance", tags=["attendance"])

# --- MODIFIÉ : Utilise la nouvelle dépendance de permission ---
@router.post("/", response_model=AttendanceOut, dependencies=[Depends(api_require_permission("can_manage_absences"))])
# --- FIN MODIFIÉ ---
async def create_attendance(
    payload: AttendanceCreate, 
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user) # Renommé
):
    """Log a new attendance record (e.g., absence)."""
    
    # Validation
    res = await db.execute(select(Employee).where(Employee.id == payload.employee_id))
    employee = res.scalar_one_or_none()
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")

    # --- MODIFIÉ : Vérification de permission par branche ---
    # Un non-admin ne peut agir que sur son propre magasin
    if not user.permissions.is_admin and user.branch_id != employee.branch_id:
        raise HTTPException(status_code=403, detail="Not authorized for this branch")
    # --- FIN MODIFIÉ ---

    attendance = Attendance(
        **payload.model_dump(),
        created_by=user.id
    )
    db.add(attendance)
    await db.commit()
    await db.refresh(attendance)
    return attendance
