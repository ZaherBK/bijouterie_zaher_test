from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, datetime
from typing import List
from pydantic import BaseModel
import json
from decimal import Decimal
from sqlalchemy.orm import selectinload

from ..models import (
    Deposit, Expense, User, Branch, Employee, Attendance, Leave, 
    Pay, Loan, LoanSchedule, LoanRepayment, Role, AuditLog
)
from ..auth import api_require_permission
from ..deps import get_db, api_current_user

router = APIRouter(prefix="/api/sync", tags=["sync"])

class SqlGenRequest(BaseModel):
    schema_name: str
    coddep_expenses: int = 1
    coddep_deposits: int = 2

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if hasattr(obj, "__dict__"):
             d = obj.__dict__.copy()
             d.pop('_sa_instance_state', None)
             return d
        return super().default(obj)

@router.post("/generate-sql")
async def generate_sql(
    payload: SqlGenRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_current_user)
):
    """
    Generates a SQL script to insert today's deposits and expenses into the legacy DB.
    """
    today = date.today()
    
    # Re-query with eager loading
    deposits_query = select(Deposit).options(selectinload(Deposit.employee)).where(Deposit.date == today)
    res_deposits = await db.execute(deposits_query)
    deposits = res_deposits.scalars().all()

    expenses_query = select(Expense).where(Expense.date == today)
    res_expenses = await db.execute(expenses_query)
    expenses = res_expenses.scalars().all()
    
    if not deposits and not expenses:
        raise HTTPException(status_code=400, detail="Aucune donnée à synchroniser pour aujourd'hui.")

    # Generate SQL
    sql_lines = []
    sql_lines.append(f"USE `{payload.schema_name}`;")
    sql_lines.append("-- Sync generated from Cloud App")
    sql_lines.append("-- --------------------------------")
    
    sql_lines.append("SELECT @max_num := MAX(NUMDEP) FROM fdepense;")
    sql_lines.append("SET @next_num := IFNULL(@max_num, 0);")
    
    for d in deposits:
        lib_dep = f"{d.employee.first_name} {d.employee.last_name}"
        if d.note:
            lib_dep += f" - {d.note}"
        lib_dep = lib_dep[:100].replace("'", "''") # Escape quotes
        
        sql_lines.append(f"SET @next_num := @next_num + 1;")
        sql_lines.append(f"""INSERT INTO fdepense (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL) VALUES (0, 'Espèces', '', '', '{d.date}', {payload.coddep_deposits}, @next_num, {d.amount}, '{lib_dep}', '{d.date}', 0, '{user.full_name}');""")

    for e in expenses:
        lib_dep = e.description
        lib_dep = lib_dep[:100].replace("'", "''")
        
        sql_lines.append(f"SET @next_num := @next_num + 1;")
        sql_lines.append(f"""INSERT INTO fdepense (TYPE, MODREG, BANQUE, NUMPCE, DATPCE, CODDEP, NUMDEP, MONTANT, LIBDEP, DATDEP, NUM, UTIL) VALUES (0, 'Espèces', '', '', '{e.date}', {payload.coddep_expenses}, @next_num, {e.amount}, '{lib_dep}', '{e.date}', 0, '{user.full_name}');""")

    sql_content = "\n".join(sql_lines)
    
    return Response(
        content=sql_content,
        media_type="application/sql",
        headers={"Content-Disposition": f"attachment; filename=sync_{today}.sql"}
    )

@router.get("/backup")
async def export_backup(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(api_require_permission("is_admin"))
):
    """
    Export full cloud database as JSON (API version for AutoSync).
    """
    data_to_export = {}
    
    try:
        # Fetch all tables
        data_to_export["branches"] = (await db.execute(select(Branch))).scalars().all()
        data_to_export["users"] = (await db.execute(select(User))).scalars().all()
        data_to_export["roles"] = (await db.execute(select(Role))).scalars().all()
        data_to_export["employees"] = (await db.execute(select(Employee))).scalars().all()
        data_to_export["attendance"] = (await db.execute(select(Attendance))).scalars().all()
        data_to_export["leaves"] = (await db.execute(select(Leave))).scalars().all()
        data_to_export["deposits"] = (await db.execute(select(Deposit))).scalars().all()
        data_to_export["expenses"] = (await db.execute(select(Expense))).scalars().all() 
        data_to_export["pay_history"] = (await db.execute(select(Pay))).scalars().all()
        data_to_export["loans"] = (await db.execute(select(Loan))).scalars().all()
        data_to_export["loan_schedules"] = (await db.execute(select(LoanSchedule))).scalars().all()
        data_to_export["loan_repayments"] = (await db.execute(select(LoanRepayment))).scalars().all()
        data_to_export["audit_logs"] = (await db.execute(select(AuditLog).order_by(AuditLog.created_at))).scalars().all()
        
        # Serialize
        json_content = json.dumps(data_to_export, cls=CustomJSONEncoder, indent=2, ensure_ascii=False)
        
        filename = f"cloud_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return Response(
            content=json_content,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
