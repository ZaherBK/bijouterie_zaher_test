from datetime import date
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload # <--- AJOUTÉ

from app.deps import get_db
from app.auth import api_require_permission
from app.models import (
    Loan, LoanSchedule, LoanRepayment,
    LoanInterestType, LoanTermUnit, LoanStatus, ScheduleStatus, RepaymentSource,
    Employee, LoanSettings
)
from app.schemas import LoanCreate, LoanOut, RepaymentCreate, RepaymentOut, LoanScheduleOut
from app.services.loan_calc import build_schedule, recompute_derived

router = APIRouter(prefix="/api/loans", tags=["loans"])

# Helper: DTI eligibility
async def _check_eligibility(db: AsyncSession, employee_id: int, amount_per_term: Decimal, unit: LoanTermUnit):
    # salary من Employee (موجود في موديلك)
    emp = await db.get(Employee, employee_id)
    if not emp or not emp.active:
        raise HTTPException(400, "Employee not eligible")

    salary = Decimal(emp.salary or 0)
    if salary <= 0:
        return  # لو مش مدخّل راتب، نسمح ونترك القرار للمدير

    settings = (await db.execute(select(LoanSettings).limit(1))).scalar_one_or_none()
    max_dti = Decimal(settings.max_dti) if settings else Decimal("0.30")

    # نحول القسط الشهري/الأسبوعي إلى نسبة من الراتب (تقريبية)
    # (لو راتبك شهري: قسط شهري مباشر؛ قسط أسبوعي × 4.333 للتقدير)
    if unit == LoanTermUnit.week:
        monthly_equivalent = amount_per_term * Decimal("4.333333333")
    else:
        monthly_equivalent = amount_per_term

    if salary > 0 and (monthly_equivalent / salary) > max_dti:
        raise HTTPException(400, "DTI exceeds company limit")

@router.get("/", response_model=list[LoanOut], dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def list_loans(status: LoanStatus | None = None, employee_id: int | None = None, db: AsyncSession = Depends(get_db)):
    # Ensure employee is loaded to avoid Lazy Loading errors
    q = select(Loan).options(selectinload(Loan.employee), selectinload(Loan.creator))
    if status:
        q = q.where(Loan.status == status)
    if employee_id:
        q = q.where(Loan.employee_id == employee_id)
    q = q.order_by(Loan.created_at.desc())
    res = await db.execute(q)
    return res.scalars().all()

@router.post("/", response_model=LoanOut, dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def create_loan(payload: LoanCreate, db: AsyncSession = Depends(get_db), user=Depends(api_require_permission("can_manage_loans"))):
    # منع أكثر من قرض نشط لو إعداد الشركة يطلب ذلك
    settings = (await db.execute(select(LoanSettings).limit(1))).scalar_one_or_none()
    if settings and settings.max_concurrent_loans == 1:
        exists = await db.execute(select(func.count()).select_from(Loan).where(
            Loan.employee_id == payload.employee_id,
            Loan.status.in_([LoanStatus.approved, LoanStatus.active])
        ))
        if exists.scalar_one() > 0:
            raise HTTPException(400, "Employee already has an active loan")

    loan = Loan(
        employee_id=payload.employee_id,
        principal=payload.principal,
        interest_type=LoanInterestType(payload.interest_type),
        annual_interest_rate=payload.annual_interest_rate,
        term_count=payload.term_count,
        term_unit=LoanTermUnit(payload.term_unit),
        start_date=payload.start_date,
        first_due_date=payload.first_due_date,
        fee=payload.fee,
        notes=payload.notes,
        status=LoanStatus.draft,
        created_by=user["id"]
    )
    db.add(loan)
    await db.flush()  # نحتاج id لتوليد الجدول

    rows = build_schedule(loan)
    
    # Check DTI (Désactivé comme demandé)
    if rows and False: 
        # await _check_eligibility(db, loan.employee_id, rows[0].due_total, loan.term_unit)
        pass

    for r in rows:
        db.add(r)

    # --- DEBUT DE LA CORRECTION ---
    # NE PAS FAIRE CECI - CELA CAUSE LE CRASH "MissingGreenlet" / "TypeError"
    # loan.schedules = rows 
    
    # عند الموافقة مباشرةً
    loan.status = LoanStatus.approved
    
    # Passer 'rows' en argument pour éviter le lazy-load et le TypeError
    recompute_derived(loan, schedules=rows)

    # PAS de commit ici ! get_db (de db.py) s'en occupe.
    # await db.commit()
    # await db.refresh(loan)
    # --- FIN DE LA CORRECTION ---
    
    return loan

@router.get("/{loan_id}", response_model=LoanOut, dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def get_loan(loan_id: int, db: AsyncSession = Depends(get_db)):
    # --- AJOUTÉ .options(...) pour charger l'employé ---
    loan = (await db.execute(
        select(Loan).options(selectinload(Loan.employee)).where(Loan.id == loan_id)
    )).scalar_one_or_none()
    
    if not loan:
        raise HTTPException(404, "Loan not found")
    return loan

@router.get("/{loan_id}/schedule", response_model=list[LoanScheduleOut], dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def get_schedule(loan_id: int, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(LoanSchedule).where(LoanSchedule.loan_id == loan_id).order_by(LoanSchedule.sequence_no))
    return res.scalars().all()

@router.post("/{loan_id}/approve", response_model=LoanOut, dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def approve_loan(loan_id: int, db: AsyncSession = Depends(get_db)):
    loan = await db.get(Loan, loan_id)
    if not loan:
        raise HTTPException(404, "Loan not found")
    if loan.status != LoanStatus.approved and loan.status != LoanStatus.draft:
        raise HTTPException(400, "Loan already processed")

    loan.status = LoanStatus.active
    # PAS de commit ici ! get_db s'en occupe.
    return loan

@router.post("/{loan_id}/repay", response_model=RepaymentOut, dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def repay(loan_id: int, payload: RepaymentCreate, db: AsyncSession = Depends(get_db), user=Depends(api_require_permission("can_manage_loans"))):
    # --- AJOUTÉ .options(...) pour charger les schedules ---
    loan = (await db.execute(
        select(Loan).options(selectinload(Loan.schedules)).where(Loan.id == loan_id)
    )).scalar_one_or_none()
    # --- FIN AJOUT ---

    if not loan or loan.status not in [LoanStatus.active, LoanStatus.approved]:
        raise HTTPException(400, "Loan not active")

    target = None
    if payload.schedule_id:
        # Chercher dans les schedules déjà chargés
        target = next((s for s in loan.schedules if s.id == payload.schedule_id), None)
        if not target:
            raise HTTPException(400, "Invalid schedule")
    else:
        # Chercher le premier non payé dans la liste chargée
        target = next(
            (s for s in sorted(loan.schedules, key=lambda x: x.sequence_no)
             if s.status in (ScheduleStatus.overdue, ScheduleStatus.partial, ScheduleStatus.pending)),
            None
        )

    if not target:
        raise HTTPException(400, "Nothing to pay")

    remaining = target.due_total - target.paid_total
    pay_amount = payload.amount if payload.amount <= remaining else remaining

    target.paid_total += pay_amount
    
    # --- CORRECTION Division par zéro ---
    remaining_due_before_pay = (target.due_total - (target.paid_total - pay_amount))
    if remaining_due_before_pay > 0:
        # Gérer le cas où due_principal ou paid_principal est None
        due_principal = target.due_principal or 0
        paid_principal = target.paid_principal or 0
        due_interest = target.due_interest or 0
        paid_interest = target.paid_interest or 0

        p_ratio = (due_principal - paid_principal) / remaining_due_before_pay
        i_ratio = (due_interest - paid_interest) / remaining_due_before_pay
        
        target.paid_principal = paid_principal + (pay_amount * p_ratio)
        target.paid_interest = paid_interest + (pay_amount * i_ratio)
    else:
        # Si le paiement restant était 0, mettez tout sur le principal
        target.paid_principal = (target.paid_principal or 0) + pay_amount
    # --- FIN CORRECTION ---

    if target.paid_total >= target.due_total:
        target.status = ScheduleStatus.paid
        target.paid_on = payload.paid_on
    else:
        target.status = ScheduleStatus.partial

    repayment = LoanRepayment(
        loan_id=loan_id, schedule_id=target.id, amount=pay_amount,
        source=payload.source, paid_on=payload.paid_on, notes=payload.notes, created_by=user["id"]
    )
    db.add(repayment)

    # تحديث المشتقات
    # 'loan.schedules' est déjà chargé, on peut le passer directement
    recompute_derived(loan, schedules=loan.schedules)

    # PAS de commit ici ! get_db s'en occupe.
    
    # Nous devons flush pour obtenir l'ID du remboursement
    await db.flush()
    await db.refresh(repayment)
    return repayment

@router.post("/{loan_id}/cancel", response_model=LoanOut, dependencies=[Depends(api_require_permission("can_manage_loans"))])
async def cancel_loan(loan_id: int, db: AsyncSession = Depends(get_db)):
    loan = await db.get(Loan, loan_id)
    if not loan:
        raise HTTPException(404, "Loan not found")
    if loan.repaid_total > 0:
        # Add the closing quote and parenthesis
        raise HTTPException(400, "Cannot cancel a loan that has repayments")
    
    # You will likely need to add the rest of the function logic here
    # For example:
    # loan.status = LoanStatus.cancelled
    # return loan
