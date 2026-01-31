from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from sqlalchemy.orm import selectinload
from app.models import Employee, Attendance, AttendanceType, Deposit, SalesSummary, Pay, PayType, Loan, ScheduleStatus, LoanSchedule

class PayrollService:
    @staticmethod
    async def get_payroll_stats(
        db: AsyncSession,
        start_date: date,
        end_date: date,
        branch_id: Optional[int] = None
    ):
        """
        Calculate payroll statistics for all active employees within a date range.
        Standardizes logic for calculating absences, advances, and sales performance.
        """
        
        # 1. Fetch Active Employees (filtered by branch if provided)
        # Use selectinload to eagerly load the 'branch' relationship to avoid async errors
        stmt_emp = select(Employee).options(selectinload(Employee.branch)).where(Employee.active == True)
        if branch_id:
            stmt_emp = stmt_emp.where(Employee.branch_id == branch_id)
        
        res_emp = await db.execute(stmt_emp)
        employees = res_emp.scalars().all()
        employee_ids = [e.id for e in employees]
        
        if not employee_ids:
            return []

        # 2. Aggregations (Absences, Advances, Sales) using Subqueries or separate queries
        # (Using separate queries here for clarity and easier maintenance, similar perf for small datasets)

        # A. Absences count
        stmt_abs = (
            select(
                Attendance.employee_id,
                func.count(Attendance.id).label("absence_count")
            )
            .where(
                Attendance.employee_id.in_(employee_ids),
                Attendance.atype == AttendanceType.absent,
                Attendance.date.between(start_date, end_date)
            )
            .group_by(Attendance.employee_id)
        )
        res_abs = await db.execute(stmt_abs)
        abs_map = {row.employee_id: row.absence_count for row in res_abs.all()}

        # B. Advances (Deposits) sum
        stmt_adv = (
            select(
                Deposit.employee_id,
                func.sum(Deposit.amount).label("avance_total")
            )
            .where(
                Deposit.employee_id.in_(employee_ids),
                Deposit.date.between(start_date, end_date)
            )
            .group_by(Deposit.employee_id)
        )
        res_adv = await db.execute(stmt_adv)
        adv_map = {row.employee_id: row.avance_total for row in res_adv.all()}

        # C. Sales stats (Qty + Rev)
        # Note: If branch_id is set, we might want to filter sales by store_name matches branch name
        # but for global payroll, we usually want TOTAL sales per employee regardless of where they sold.
        stmt_sales = (
            select(
                SalesSummary.employee_id,
                func.sum(SalesSummary.quantity_sold).label("total_qty"),
                func.sum(SalesSummary.total_revenue).label("total_rev")
            )
            .where(
                SalesSummary.employee_id.in_(employee_ids),
                SalesSummary.date.between(start_date, end_date)
            )
            .group_by(SalesSummary.employee_id)
        )
        res_sales = await db.execute(stmt_sales)
        sales_map = {
            row.employee_id: {"qty": row.total_qty, "rev": row.total_rev} 
            for row in res_sales.all()
        }
        
        # D. Loan Schedules Due (Approved & Pending/Partial) in range
        # Matches logic from employee_report_index
        stmt_loans = (
             select(
                LoanSchedule.loan_id,
                func.sum(LoanSchedule.due_total - LoanSchedule.paid_total).label("due_amount"),
                Loan.employee_id
            )
            .join(Loan, LoanSchedule.loan_id == Loan.id)
            .where(
                Loan.employee_id.in_(employee_ids),
                LoanSchedule.due_date.between(start_date, end_date),
                LoanSchedule.status.in_([ScheduleStatus.pending, ScheduleStatus.partial])
            )
            .group_by(Loan.employee_id, LoanSchedule.loan_id)
        )
        res_loans = await db.execute(stmt_loans)
        loan_map = {} 
        for row in res_loans.all():
            current = loan_map.get(row.employee_id, Decimal(0))
            loan_map[row.employee_id] = current + (row.due_amount or Decimal(0))


        # 3. Build Result List
        results = []
        for emp in employees:
            salary = emp.salary or Decimal(0)
            absences = abs_map.get(emp.id, 0)
            
            # Simple Deduction Logic (can be enhanced)
            # Assuming 26 working days for monthly? Or using a daily rate.
            # Simplified: deduction = (salary / 26) * absences
            daily_rate = salary / Decimal(26) if salary > 0 else Decimal(0)
            deduction = daily_rate * Decimal(absences)
            
            advances = adv_map.get(emp.id, Decimal(0)) or Decimal(0)
            loans_due = loan_map.get(emp.id, Decimal(0))
            
            sales_data = sales_map.get(emp.id, {"qty": 0, "rev": 0})
            
            # Estimated Net (excluding Primes for now, as they are variable)
            # Net = Salary - Deduction - Advances - Loans
            net_estimated = salary - deduction - advances - loans_due
            
            results.append({
                "employee": emp,
                "stats": {
                    "salary": salary,
                    "absences": absences,
                    "deduction": deduction,
                    "advances": advances,
                    "loans": loans_due,
                    "sales_qty": sales_data["qty"],
                    "sales_rev": sales_data["rev"],
                    "net_estimated": net_estimated
                }
            })
            
        return results
