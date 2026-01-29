from datetime import date
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from ..db import get_db
from ..models import SalesSummary, Employee, User
from ..schemas import SalesSummaryCreate, SalesSummaryOut

router = APIRouter(
    prefix="/sales",
    tags=["sales"],
    responses={404: {"description": "Not found"}},
)

@router.post("/sync", status_code=status.HTTP_201_CREATED)
def sync_sales_summary(
    summaries: List[SalesSummaryCreate],
    db: Session = Depends(get_db),
    # token: str = Depends(oauth2_scheme) # Secured by API Key later? For now open or Basic Auth if needed. User has Token.
):
    """
    Receive Daily Sales Summaries from Local Agent.
    """
    synced_count = 0
    
    for item in summaries:
        # 1. Try to find Employee Mapping (First Name Match)
        # We assume local_user_name is "First Name" (e.g. "ANISSA")
        # Cloud First Name might be "Anissa".
        
        employee = db.query(Employee).filter(
            func.lower(Employee.first_name) == item.local_user_name.lower().strip()
        ).first()
        
        emp_id = employee.id if employee else None
        
        # 2. Check if record exists (Upsert)
        existing = db.query(SalesSummary).filter(
            SalesSummary.date == item.date,
            SalesSummary.local_user_name == item.local_user_name,
            SalesSummary.store_name == item.store_name
        ).first()
        
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
    
    db.commit()
    return {"message": f"Synced {synced_count} sales records"}

@router.get("/", response_model=List[SalesSummaryOut])
def get_sales(
    start_date: date,
    end_date: date,
    db: Session = Depends(get_db)
):
    return db.query(SalesSummary).filter(
        SalesSummary.date >= start_date,
        SalesSummary.date <= end_date
    ).all()
