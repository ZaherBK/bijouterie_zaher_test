from datetime import date, datetime, timedelta
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
            
            # --- SCHEDULE (Work Days) ---
            # Default Mon-Sat (0-5) if missing
            work_days_str = emp.work_days if hasattr(emp, 'work_days') and emp.work_days else "0,1,2,3,4,5"
            try:
                scheduled_days = {int(d) for d in work_days_str.split(',') if d.strip()}
            except:
                scheduled_days = {0,1,2,3,4,5}

            # --- Calculate Actual Working Days in Period ---
            # Used for Exact Daily Rate
            actual_work_days_count = 0
            curr = start_date
            while curr <= end_date:
                if curr.weekday() in scheduled_days:
                    actual_work_days_count += 1
                curr += timedelta(days=1)
            
            # --- Leaves (calc unpaid days) ---
            emp_leaves = leave_map[emp.id]
            unpaid_leave_days = 0
            for l in emp_leaves:
                if l.ltype.value in ['unpaid', 'sick_unpaid']:
                    # Calculate DURATION based on SCHEDULE
                    curr = l.start_date
                    while curr <= l.end_date:
                        if curr.weekday() in scheduled_days: 
                            unpaid_leave_days += 1
                        curr += timedelta(days=1)
            
            # --- Deduction ---
            # Deduction covers Absences (AttendanceType.absent) AND Unpaid Leaves
            total_deduction_days = abs_count + unpaid_leave_days
            
            # --- Exact Daily Rate & Deduction Calc ---
            # 1. Monthly Frequency (Legacy/Specific cases)
            if emp.salary_frequency == "monthly":
                if actual_work_days_count > 0 and salary > 0:
                    daily_rate = salary / Decimal(actual_work_days_count)
                else:
                    daily_rate = Decimal(0)
            
            # 2. Weekly Frequency (New Standard: Salary / 4)
            else:
                # "Salary" in DB is Monthly Base
                weekly_base = salary / Decimal(4)
                
                # Count scheduled days per week (e.g. 6 days)
                days_per_week = len(scheduled_days) if scheduled_days else 6 
                
                if days_per_week > 0:
                    daily_rate = weekly_base / Decimal(days_per_week)
                else:
                    daily_rate = Decimal(0)

            # Deduction
            deduction = daily_rate * Decimal(total_deduction_days)
            
            # --- Advances ---
            emp_advances = adv_map[emp.id]
            total_advances = sum((a.amount for a in emp_advances), Decimal(0))
            
            # --- Loans ---
            loans_due = loan_due_map.get(emp.id, Decimal(0))
            
            # --- Sales ---
            sales_data = sales_map.get(emp.id, {"qty": 0, "rev": 0})
            
            # --- Gross Pay Calculation ---
            # If Monthly: Gross = Monthly Salary
            # If Weekly: Gross depends on how many weeks in the report range?
            # User wants "Weekly Pay = Salary / 4".
            # If report is 1 week -> Show 250.
            # If report is 1 month -> Show 1000? 
            # Or always show Weekly output? The report is "Global Payroll".
            # Let's assume standard behavior:
            # - Calculate "Earnable Pay" for the selected PERIOD.
            
            if emp.salary_frequency == "monthly":
                # For monthly, we usually assume the full salary is for the full month
                # But if range is partial, should we prorate?
                # Sticking to simple logic: Show Full Salary as Base, Deduct absences.
                gross_pay = salary
            else:
                # Weekly Logic
                # We need to know how many "Weeks" are in the start_date->end_date range?
                # Or simply: Gross = DailyRate * ScheduledWorkDaysInPeriod
                # This ensures:
                # 1 Week Report (6 work days) -> 41.66 * 6 = 250 (Correct)
                # 4 Week Report (24 work days) -> 41.66 * 24 = 1000 (Correct)
                
                # We use actual_work_days_count calculated above (based on schedule)
                # BUT 'actual_work_days_count' is what they SHOULD work.
                # So Gross Potential = DailyRate * actual_work_days_count
                
                gross_pay = daily_rate * Decimal(actual_work_days_count)

            # --- Net ---
            net_estimated = gross_pay - deduction - total_advances - loans_due
            
            results.append({
                "employee": emp,
                "stats": {
                    "salary": gross_pay, # Show the Prorated/Period Salary, not just Base
                    "base_salary": salary, # Keep track of contract salary if needed
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
