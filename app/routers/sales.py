from datetime import date
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from ..db import get_db
from ..models import SalesSummary, Employee, User
from ..schemas import SalesSummaryCreate, SalesSummaryOut


router = APIRouter(
    prefix="/api/sales",
    tags=["sales"],
    responses={404: {"description": "Not found"}},
)

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

@router.post("/sync", status_code=status.HTTP_201_CREATED)
async def sync_sales_summary(
    summaries: List[SalesSummaryCreate],
    db: AsyncSession = Depends(get_db),
    # token: str = Depends(oauth2_scheme) 
):
    """
    Receive Daily Sales Summaries from Local Agent.
    """
    synced_count = 0
    
    for item in summaries:
        # 1. Try to find Employee Mapping (First Name Match)
        stmt = select(Employee).where(
            func.lower(Employee.first_name) == item.local_user_name.lower().strip()
        )
        result = await db.execute(stmt)
        employee = result.scalar_one_or_none()
        
        emp_id = employee.id if employee else None
        
        # 2. Check if record exists (Upsert)
        stmt_exist = select(SalesSummary).where(
            SalesSummary.date == item.date,
            SalesSummary.local_user_name == item.local_user_name,
            SalesSummary.store_name == item.store_name
        )
        result_exist = await db.execute(stmt_exist)
        existing = result_exist.scalar_one_or_none()
        
        if existing:
            # Update
            existing.quantity_sold = item.quantity_sold
            existing.total_revenue = item.total_revenue
            existing.employee_id = emp_id # Update link if new employee added
        else:
            # Create
            new_record = SalesSummary(
                date=item.date,
                local_user_name=item.local_user_name,
                store_name=item.store_name,
                quantity_sold=item.quantity_sold,
                total_revenue=item.total_revenue,
                employee_id=emp_id
            )
            db.add(new_record)
            
        synced_count += 1
    
    await db.commit()
    return {"message": f"Synced {synced_count} sales records"}

@router.get("/", response_model=List[SalesSummaryOut])
async def get_sales(
    start_date: date,
    end_date: date,
    db: AsyncSession = Depends(get_db)
):
    stmt = select(SalesSummary).where(
        SalesSummary.date >= start_date,
        SalesSummary.date <= end_date
    )
    result = await db.execute(stmt)
    return result.scalars().all()
