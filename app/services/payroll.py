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

        # 2. Fetch Detailed Records (Absences, Advances, Leaves, Loans, Sales)
        # Using separate queries for cleaner code (could be heavily joined, but this is clearer for maintenance)

        # A. Absences (List of dates/notes)
        stmt_abs = (
            select(Attendance)
            .where(
                Attendance.employee_id.in_(employee_ids),
                Attendance.atype == AttendanceType.absent,
                Attendance.date.between(start_date, end_date)
            )
            .order_by(Attendance.date)
        )
        res_abs = await db.execute(stmt_abs)
        # Map: employee_id -> list[Attendance]
        abs_map = {eid: [] for eid in employee_ids}
        for row in res_abs.scalars().all():
            abs_map[row.employee_id].append(row)

        # B. Advances (List of Deposits)
        stmt_adv = (
            select(Deposit)
            .where(
                Deposit.employee_id.in_(employee_ids),
                Deposit.date.between(start_date, end_date)
            )
            .order_by(Deposit.date)
        )
        res_adv = await db.execute(stmt_adv)
        # Map: employee_id -> list[Deposit]
        adv_map = {eid: [] for eid in employee_ids}
        for row in res_adv.scalars().all():
            adv_map[row.employee_id].append(row)

        # C. Leaves (List of Leave objects) - NEW
        from app.models import Leave # Ensure imported if not already top-level
        stmt_leaves = (
            select(Leave)
            .where(
                Leave.employee_id.in_(employee_ids),
                # Check overlapping range or starts within range? 
                # Usually: any part of leave falls in month. 
                # Simple logic: start_date between provided range (for payroll view)
                Leave.start_date.between(start_date, end_date) 
            )
            .order_by(Leave.start_date)
        )
        res_leaves = await db.execute(stmt_leaves)
        leave_map = {eid: [] for eid in employee_ids}
        for row in res_leaves.scalars().all():
            leave_map[row.employee_id].append(row)

        # D. Sales stats (Qty + Rev) - Aggregates stay useful for summary
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
        
        # E. Loan Schedules Due (Aggregated Loan Payment Due this month)
        # Keeping this as an Amount is fine, but user might want loan details?
        # For now, sticking to total Due as requested ("prêts").
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
        loan_due_map = {} 
        for row in res_loans.all():
            current = loan_due_map.get(row.employee_id, Decimal(0))
            loan_due_map[row.employee_id] = current + (row.due_amount or Decimal(0))


        # 3. Build Result List
        results = []
        for emp in employees:
            salary = emp.salary or Decimal(0)
            
            # --- Absences ---
            emp_absences = abs_map[emp.id]
            abs_count = len(emp_absences)
            
            # --- Leaves (calc unpaid days) ---
            emp_leaves = leave_map[emp.id]
            unpaid_leave_days = 0
            for l in emp_leaves:
                if l.ltype.value == 'unpaid':
                    # Calculate DURATION excluding SUNDAYS (6/7 Work Week)
                    # We iterate through days to verify specific off-days
                    curr = l.start_date
                    while curr <= l.end_date:
                        # 0=Mon, 6=Sun. Skip Sunday.
                        if curr.weekday() != 6: 
                            unpaid_leave_days += 1
                        curr += timedelta(days=1)
            
            # --- Deduction ---
            # Deduction covers Absences (AttendanceType.absent) AND Unpaid Leaves
            total_deduction_days = abs_count + unpaid_leave_days
            
            # Assuming 26 working days for monthly deduction
            daily_rate = salary / Decimal(26) if salary > 0 else Decimal(0)
            deduction = daily_rate * Decimal(total_deduction_days)
            
            # --- Advances ---
            emp_advances = adv_map[emp.id]
            total_advances = sum((a.amount for a in emp_advances), Decimal(0))
            
            # --- Loans ---
            loans_due = loan_due_map.get(emp.id, Decimal(0))
            
            # --- Sales ---
            sales_data = sales_map.get(emp.id, {"qty": 0, "rev": 0})
            
            # --- Net ---
            net_estimated = salary - deduction - total_advances - loans_due
            
            results.append({
                "employee": emp,
                "stats": {
                    "salary": salary,
                    # Details
                    "absences_list": emp_absences,  # List[Attendance]
                    "advances_list": emp_advances,  # List[Deposit]
                    "leaves_list": emp_leaves,      # List[Leave]
                    
                    # Summaries
                    "absences_count": abs_count,
                    "unpaid_leave_days": unpaid_leave_days,
                    "total_deduction_days": total_deduction_days,
                    "deduction_amount": deduction,
                    
                    "advances_total": total_advances,
                    "loans_due_total": loans_due,
                    "sales_qty": sales_data["qty"],
                    "sales_rev": sales_data["rev"],
                    
                    "net_estimated": net_estimated
                }
            })
            
        return results
