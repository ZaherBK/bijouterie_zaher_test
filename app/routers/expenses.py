from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, delete, func
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
    
    # Logic for Branch assignment
    target_branch_id = payload.branch_id
    if not user.permissions.is_admin:
        # Managers are forced to their own branch
        target_branch_id = user.branch_id
    
    # Create Expense
    # Exclude branch_id from payload dump to manually set it
    expense_data = payload.model_dump(exclude={"branch_id"})
    
    expense = Expense(
        **expense_data,
        branch_id=target_branch_id,
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
    branch_id: int | None = None, # Admin filter
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user)
):
    """List all expenses."""
    query = select(Expense).options(selectinload(Expense.creator)).order_by(Expense.date.desc(), Expense.created_at.desc())
    
    # Permission Check
    if not user.permissions.is_admin:
        # Filter by user's branch
        # Logic: (Expense.branch_id == user.branch_id) OR (Expense.branch_id IS NULL AND Creator.branch_id == user.branch_id)
        from sqlalchemy import or_, and_
        query = query.outerjoin(User, Expense.created_by == User.id)
        query = query.where(
            or_(
                Expense.branch_id == user.branch_id,
                and_(Expense.branch_id.is_(None), User.branch_id == user.branch_id)
            )
        )
    else:
        # Admin: Filter if requested
        if branch_id:
             query = query.where(Expense.branch_id == branch_id)
        
    # --- SELF-HEALING: Auto-Migrate if column missing ---
    from sqlalchemy.exc import ProgrammingError
    from sqlalchemy import text
    import logging

    try:
        res = await db.execute(query.limit(100))
    except ProgrammingError as e:
        # Check if error is 'UndefinedColumnError' regarding 'branch_id'
        if "branch_id" in str(e) and ("does not exist" in str(e) or "UndefinedColumnError" in str(e)):
            logging.error("Self-Healing: Detected missing 'expenses.branch_id' column. Attempting auto-migration.")
            try:
                # Force migration
                await db.execute(text("ALTER TABLE expenses ADD COLUMN branch_id INTEGER REFERENCES branches(id)"))
                await db.commit()
                logging.info("Self-Healing: Migration successful. Retrying query.")
                
                # Retry the query
                res = await db.execute(query.limit(100))
            except Exception as migration_error:
                logging.critical(f"Self-Healing Failed: {migration_error}")
                raise e # Raise original error if repair fails
        else:
            raise e
        
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
