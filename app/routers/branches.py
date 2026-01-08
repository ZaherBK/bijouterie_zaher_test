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


# --- MODIFIÉ : Endpoints de gestion (Admin Uniquement) ---
from ..schemas import BranchUpdate

@router.put("/{branch_id}", response_model=BranchOut, dependencies=[Depends(api_require_permission("can_manage_branches"))])
async def update_branch(branch_id: int, payload: BranchUpdate, db: AsyncSession = Depends(get_db)):
    """Update a branch."""
    branch = await db.get(Branch, branch_id)
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
        
    # Check name uniqueness if changed
    if payload.name != branch.name:
        exists = await db.execute(select(Branch).where(Branch.name == payload.name))
        if exists.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Branch name already exists")
            
    branch.name = payload.name
    branch.city = payload.city
    
    await db.commit()
    await db.refresh(branch)
    return branch


@router.delete("/{branch_id}", dependencies=[Depends(api_require_permission("can_manage_branches"))])
async def delete_branch(branch_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a branch."""
    branch = await db.get(Branch, branch_id)
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
        
    await db.delete(branch)
    await db.commit()
    return {"success": True}
# --- FIN MODIFIÉ ---
