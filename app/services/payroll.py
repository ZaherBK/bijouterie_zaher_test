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

        # Separate employees by salary frequency
        monthly_emps = [e for e in employees if e.salary_frequency != "weekly"]
        weekly_emps =  [e for e in employees if e.salary_frequency == "weekly"]
        
        results = []

        async def process_batch(emps_batch: List[Employee], p_start: date, p_end: date, freq_type: str):
            if not emps_batch:
                return
            batch_eids = [e.id for e in emps_batch]

            # A. Absences
            stmt_abs = (
                select(Attendance)
                .where(
                    Attendance.employee_id.in_(batch_eids),
                    Attendance.atype == AttendanceType.absent,
                    Attendance.date.between(p_start, p_end)
                )
                .order_by(Attendance.date)
            )
            res_abs = await db.execute(stmt_abs)
            abs_map = {eid: [] for eid in batch_eids}
            for row in res_abs.scalars().all():
                abs_map[row.employee_id].append(row)

            # B. Advances
            stmt_adv = (
                select(Deposit)
                .where(
                    Deposit.employee_id.in_(batch_eids),
                    Deposit.date.between(p_start, p_end)
                )
                .order_by(Deposit.date)
            )
            res_adv = await db.execute(stmt_adv)
            adv_map = {eid: [] for eid in batch_eids}
            for row in res_adv.scalars().all():
                adv_map[row.employee_id].append(row)

            # C. Leaves
            from app.models import Leave
            stmt_leaves = (
                select(Leave)
                .where(
                    Leave.employee_id.in_(batch_eids),
                    Leave.start_date.between(p_start, p_end) 
                )
                .order_by(Leave.start_date)
            )
            res_leaves = await db.execute(stmt_leaves)
            leave_map = {eid: [] for eid in batch_eids}
            for row in res_leaves.scalars().all():
                leave_map[row.employee_id].append(row)

            # D. Sales
            stmt_sales = (
                select(
                    SalesSummary.employee_id,
                    func.sum(SalesSummary.quantity_sold).label("total_qty"),
                    func.sum(SalesSummary.total_revenue).label("total_rev")
                )
                .where(
                    SalesSummary.employee_id.in_(batch_eids),
                    SalesSummary.date.between(p_start, p_end)
                )
                .group_by(SalesSummary.employee_id)
            )
            res_sales = await db.execute(stmt_sales)
            sales_map = {
                row.employee_id: {"qty": row.total_qty, "rev": row.total_rev} 
                for row in res_sales.all()
            }
            
            # E. Loans
            stmt_loans = (
                 select(
                    LoanSchedule.loan_id,
                    func.sum(LoanSchedule.due_total - LoanSchedule.paid_total).label("due_amount"),
                    Loan.employee_id
                )
                .join(Loan, LoanSchedule.loan_id == Loan.id)
                .where(
                    Loan.employee_id.in_(batch_eids),
                    LoanSchedule.due_date.between(p_start, p_end),
                    LoanSchedule.status.in_([ScheduleStatus.pending, ScheduleStatus.partial])
                )
                .group_by(Loan.employee_id, LoanSchedule.loan_id)
            )
            res_loans = await db.execute(stmt_loans)
            loan_due_map = {} 
            for row in res_loans.all():
                current = loan_due_map.get(row.employee_id, Decimal(0))
                loan_due_map[row.employee_id] = current + (row.due_amount or Decimal(0))

            # Build Results for this batch
            for emp in emps_batch:
                salary = emp.salary or Decimal(0)
                
                # Setup Display Base
                if freq_type == "weekly":
                    displayed_base_salary = salary / Decimal(4)
                else:
                    displayed_base_salary = salary
                
                emp_absences = abs_map[emp.id]
                abs_count = len(emp_absences)
                emp_advances = adv_map[emp.id]
                emp_leaves = leave_map[emp.id]
                
                # Schedule Setup
                work_days_str = emp.work_days if hasattr(emp, 'work_days') and emp.work_days else "0,1,2,3,4,5"
                try:
                    scheduled_days = {int(d) for d in work_days_str.split(',') if d.strip()}
                except:
                    scheduled_days = {0,1,2,3,4,5}

                # Work days count
                actual_work_days_count = 0
                curr = p_start
                while curr <= p_end:
                    if curr.weekday() in scheduled_days:
                        actual_work_days_count += 1
                    curr += timedelta(days=1)
                
                # Leaves Unpaid Days
                unpaid_leave_days = 0
                for l in emp_leaves:
                    if l.ltype.value in ['unpaid', 'sick_unpaid']:
                        l_start_eff = max(l.start_date, p_start)
                        l_end_eff = min(l.end_date, p_end)
                        curr = l_start_eff
                        while curr <= l_end_eff:
                            if curr.weekday() in scheduled_days: 
                                unpaid_leave_days += 1
                            curr += timedelta(days=1)
                            
                total_deduction_days = abs_count + unpaid_leave_days

                # Daily Rate
                if freq_type == "monthly":
                    if actual_work_days_count > 0 and displayed_base_salary > 0:
                        daily_rate = displayed_base_salary / Decimal(actual_work_days_count)
                    else:
                        daily_rate = Decimal(0)
                else:
                    days_per_week = len(scheduled_days) if scheduled_days else 6 
                    if days_per_week > 0:
                        daily_rate = displayed_base_salary / Decimal(days_per_week)
                    else:
                        daily_rate = Decimal(0)

                deduction = daily_rate * Decimal(total_deduction_days)
                total_advances = sum((a.amount for a in emp_advances), Decimal(0))
                loans_due = loan_due_map.get(emp.id, Decimal(0))
                sales_data = sales_map.get(emp.id, {"qty": 0, "rev": Decimal(0)})
                
                if freq_type == "monthly":
                    gross_pay = displayed_base_salary
                else:
                    gross_pay = daily_rate * Decimal(actual_work_days_count)

                net_estimated = gross_pay - deduction - total_advances - loans_due
                
                results.append({
                    "employee": emp,
                    "stats": {
                        "salary": gross_pay, 
                        "base_salary_display": displayed_base_salary,
                        "absences_list": emp_absences,
                        "advances_list": emp_advances,
                        "leaves_list": emp_leaves,
                        "absences_count": abs_count,
                        "unpaid_leave_days": unpaid_leave_days,
                        "total_deduction_days": total_deduction_days,
                        "deduction_amount": deduction,
                        "advances_total": total_advances,
                        "loans_due_total": loans_due,
                        "sales_qty": sales_data["qty"],
                        "sales_rev": sales_data["rev"],
                        "net_estimated": net_estimated,
                        "period_start": p_start,
                        "period_end": p_end
                    }
                })

        # --- PROCESS BATCHES ---
        # 1. Monthly (Uses requested dates)
        await process_batch(monthly_emps, start_date, end_date, "monthly")

        # 2. Weekly (Uses current week)
        today = date.today()
        current_week_start = today - timedelta(days=today.weekday())
        current_week_end = current_week_start + timedelta(days=6)
        
        await process_batch(weekly_emps, current_week_start, current_week_end, "weekly")
        
        # Sort results by branch name, then employee first name
        # to ensure the report looks clean
        results.sort(key=lambda x: (x["employee"].branch_id or 0, x["employee"].first_name))

        return results
