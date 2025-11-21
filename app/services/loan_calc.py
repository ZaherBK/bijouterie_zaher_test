from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional
from dateutil.relativedelta import relativedelta

# --- AJOUTÉ : Importations nécessaires ---
from app.models import (
    Loan, LoanSchedule, LoanTermUnit, LoanInterestType,
    ScheduleStatus  # Assurez-vous que ScheduleStatus est importé
)
# --- FIN AJOUT ---

Q = Decimal("0.001")    # داخلياً 3 منازل
TWO = Decimal("0.01")   # للعرض

def _round_q(x: Decimal) -> Decimal:
    return x.quantize(Q, rounding=ROUND_HALF_UP)

def _round_two(x: Decimal) -> Decimal:
    return x.quantize(TWO, rounding=ROUND_HALF_UP)

def _periods_per_year(unit: LoanTermUnit) -> int:
    return 52 if unit == LoanTermUnit.week else 12

def _next_date(d: date, unit: LoanTermUnit) -> date:
    return d + (timedelta(days=7) if unit == LoanTermUnit.week else relativedelta(months=1))

def build_schedule(loan: Loan) -> list[LoanSchedule]:
    """يرجع قائمة سطور الجدول جاهزة للـadd(). لا يضيف للـsession."""
    start = loan.first_due_date or loan.start_date
    dates = [start]
    for _ in range(loan.term_count - 1):
        dates.append(_next_date(dates[-1], loan.term_unit))

    P = _round_q(loan.principal)
    r_annual = loan.annual_interest_rate or Decimal("0")
    r_period = (r_annual / Decimal(_periods_per_year(loan.term_unit))) if loan.interest_type != LoanInterestType.none else Decimal("0")

    rows: list[LoanSchedule] = []

    if loan.interest_type == LoanInterestType.none:
        # قسط أصل متساوي
        base = _round_q(P / loan.term_count)
        principal_left = P
        for i, due in enumerate(dates, start=1):
            principal = base if i < loan.term_count else _round_q(principal_left)
            interest = Decimal("0")
            total = _round_q(principal + interest)
            rows.append(LoanSchedule(
                loan_id=loan.id, sequence_no=i, due_date=due,
                due_principal=principal, due_interest=interest, due_total=total))
            principal_left = _round_q(principal_left - principal)

    elif loan.interest_type == LoanInterestType.flat:
        term_months = (loan.term_count if loan.term_unit == LoanTermUnit.month else Decimal(loan.term_count) / Decimal("4.333333333"))
        total_interest = _round_q(P * (r_annual or Decimal("0")) * (Decimal(term_months) / Decimal("12")))
        per_int = _round_q(total_interest / loan.term_count)
        base = _round_q(P / loan.term_count)
        principal_left = P
        for i, due in enumerate(dates, start=1):
            principal = base if i < loan.term_count else _round_q(principal_left)
            interest = per_int if i < loan.term_count else _round_q(total_interest - per_int * (loan.term_count - 1))
            total = _round_q(principal + interest)
            rows.append(LoanSchedule(
                loan_id=loan.id, sequence_no=i, due_date=due,
                due_principal=principal, due_interest=interest, due_total=total))
            principal_left = _round_q(principal_left - principal)

    else:   # reducing (annuity)
        if r_period <= 0:
            raise ValueError("Reducing interest requires positive annual_interest_rate")
        n = loan.term_count
        r = r_period
        A = _round_q(P * r / (Decimal("1") - (Decimal("1") + r) ** Decimal(-n)))
        balance = P
        for i, due in enumerate(dates, start=1):
            interest = _round_q(balance * r)
            principal = _round_q(A - interest) if i < n else _round_q(balance)
            total = _round_q(principal + interest)
            rows.append(LoanSchedule(
                loan_id=loan.id, sequence_no=i, due_date=due,
                due_principal=principal, due_interest=interest, due_total=total))
            balance = _round_q(balance - principal)

    # رسوم لمرة واحدة تُضاف على أول قسط (اختياري)
    if loan.fee and loan.fee > 0:
        rows[0].due_total = _round_q(rows[0].due_total + loan.fee)

    return rows

#
# --- DEBUT DE LA CORRECTION ---
#
def recompute_derived(loan: Loan, schedules: Optional[List[LoanSchedule]] = None):
    """
    تحديث الحقول المشتقة على الكائن (دون commit).
    Utilise la liste 'schedules' fournie pour éviter le lazy-loading.
    """

    # Utilise la liste fournie si elle existe, sinon utilise la relation
    schedules_list = schedules if schedules is not None else loan.schedules

    # Correction du TypeError: utilise "or 0" pour gérer les None
    scheduled_total = sum((s.due_total or 0 for s in schedules_list), Decimal("0"))
    repaid_total = sum((s.paid_total or 0 for s in schedules_list), Decimal("0"))
    
    # Correction logique et TypeError: Le principal restant est le principal total
    # moins ce qui a été payé sur le principal.
    outstanding_principal = loan.principal - sum((s.paid_principal or 0 for s in schedules_list), Decimal("0"))

    next_due = None
    # Utilise les Enums importés
    for s in sorted(schedules_list, key=lambda x: (x.status != ScheduleStatus.paid, x.sequence_no)):
        # Correction: s.status peut être None pour les nouveaux objets
        status = s.status or ScheduleStatus.pending 
        if status in (ScheduleStatus.pending, ScheduleStatus.partial, ScheduleStatus.overdue):
            next_due = s.due_date
            break
            
    loan.scheduled_total = _round_q(scheduled_total)
    loan.repaid_total = _round_q(repaid_total)
    loan.outstanding_principal = _round_q(outstanding_principal)
    loan.next_due_on = next_due
#
# --- FIN DE LA CORRECTION ---
#
