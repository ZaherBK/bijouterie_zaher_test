from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, delete
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from datetime import date
from pydantic import BaseModel

from ..schemas import ExpenseCreate, ExpenseOut
from ..models import Expense, User
from ..auth import api_require_permission
from ..deps import get_db, api_current_user
from ..services.legacy_sync import sync_deposits_to_legacy
from ..audit import log

router = APIRouter(prefix="/api/expenses", tags=["expenses"])

# --- Sync Request Schema ---
class SyncRequest(BaseModel):
    host: str = "localhost"
    schema_name: str
    password: str = "6165"
    coddep: int = 1 # Default to 1 for Expenses

@router.post("/", response_model=ExpenseOut, dependencies=[Depends(api_require_permission("can_manage_expenses"))])
async def create_expense(
    payload: ExpenseCreate, 
    db: AsyncSession = Depends(get_db), 
    user: User = Depends(api_current_user)
):
    """Create a new expense."""
    expense = Expense(
        **payload.model_dump(),
        created_by=user.id
    )
    db.add(expense)
    await db.commit()
    await db.refresh(expense)
    
    await log(
        db, user.id, "create", "expense", expense.id,
        user.branch_id, f"Dépense créée: {payload.description} ({payload.amount} TND)"
    )
    
    return expense

@router.get("/", response_model=List[ExpenseOut])
async def list_expenses(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user)
):
    """List all expenses."""
    query = select(Expense).options(selectinload(Expense.creator)).order_by(Expense.date.desc(), Expense.created_at.desc())
    
    # Permission Check
    if not user.permissions.is_admin:
        # Filter by user's branch
        query = query.join(User, Expense.created_by == User.id).where(User.branch_id == user.branch_id)
        
    res = await db.execute(query.limit(100))
    return res.scalars().all()

@router.post("/{expense_id}/delete", dependencies=[Depends(api_require_permission("can_manage_expenses"))])
async def delete_expense(
    expense_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user)
):
    """Delete an expense."""
    res = await db.execute(select(Expense).where(Expense.id == expense_id))
    expense = res.scalar_one_or_none()
    
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
        
    await db.delete(expense)
    await db.commit()
    
    await log(
        db, user.id, "delete", "expense", expense_id,
        user.branch_id, f"Dépense supprimée: {expense.description}"
    )
    
    return {"success": True}

@router.post("/sync", dependencies=[Depends(api_require_permission("can_view_settings"))])
async def sync_expenses(
    payload: SyncRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user)
):
    """
    Sync today's expenses to the legacy MySQL database.
    """
    today = date.today()
    
    # Fetch today's expenses
    query = select(Expense).where(Expense.date == today)
    res = await db.execute(query)
    expenses = res.scalars().all()
    
    if not expenses:
        return {"success": False, "message": "Aucune dépense trouvée pour aujourd'hui."}
        
    # Prepare data for sync service
    expenses_data = []
    for exp in expenses:
        expenses_data.append({
            "amount": float(exp.amount),
            "date": exp.date,
            "description": exp.description,
            # No employee_name or note for expenses, just description
        })
        
    # Call the sync service
    result = sync_deposits_to_legacy(
        host=payload.host,
        schema=payload.schema_name,
        password=payload.password,
        deposits=expenses_data,
        user_name=user.full_name,
        coddep_override=payload.coddep # Use the provided CODDEP (default 1)
    )
    
    return result
