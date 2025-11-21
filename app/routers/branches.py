from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..schemas import BranchCreate, BranchOut
from ..models import Branch
# --- MODIFIÉ ---
from ..auth import api_require_permission
# --- FIN MODIFIÉ ---
from ..deps import get_db

router = APIRouter(prefix="/api/branches", tags=["branches"])

# --- MODIFIÉ : Utilise la nouvelle dépendance de permission ---
@router.post("/", response_model=BranchOut, dependencies=[Depends(api_require_permission("can_manage_branches"))])
# --- FIN MODIFIÉ ---
async def create_branch(payload: BranchCreate, db: AsyncSession = Depends(get_db)):
    """Create a new branch. (Admin Only)"""
    exists = await db.execute(select(Branch).where(Branch.name == payload.name))
    if exists.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Branch name already exists")
    branch = Branch(**payload.model_dump())
    db.add(branch)
    await db.commit()
    await db.refresh(branch)
    return branch


@router.get("/", response_model=list[BranchOut])
async def list_branches(db: AsyncSession = Depends(get_db)):
    """List all branches."""
    res = await db.execute(select(Branch))
    return res.scalars().all()
