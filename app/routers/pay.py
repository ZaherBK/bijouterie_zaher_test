from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from ..schemas import PayOut
from ..models import Pay, Employee
from ..auth import api_require_permission
from ..deps import get_db

router = APIRouter(prefix="/api/pay", tags=["pay"])

@router.get("/", response_model=List[PayOut])
async def list_payments(db: AsyncSession = Depends(get_db)):
    """List all payments."""
    res = await db.execute(select(Pay).options(selectinload(Pay.employee), selectinload(Pay.creator)).order_by(Pay.date.desc()))
    return res.scalars().all()
