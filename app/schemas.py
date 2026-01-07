"""
Modèles Pydantic (schémas) pour la validation des requêtes et réponses.
"""
from datetime import date, datetime
from typing import List, Optional, Literal
from decimal import Decimal

from pydantic import BaseModel, EmailStr, Field, field_validator, ConfigDict

# Importer les Enums depuis models.py, y compris PayType
# --- MODIFIÉ : Role n'est plus un Enum ---
from .models import AttendanceType, LeaveType, PayType
# --- FIN MODIFIÉ ---


# --- NOUVEAUX SCHÉMAS : Role ---
class RoleBase(BaseModel):
    name: str
    is_admin: bool = False
    can_manage_users: bool = False
    can_manage_roles: bool = False
    can_manage_branches: bool = False
    can_view_settings: bool = False
    can_clear_logs: bool = False
    can_manage_employees: bool = False
    can_view_reports: bool = False
    can_manage_pay: bool = False
    can_manage_absences: bool = False
    can_manage_leaves: bool = False
    can_manage_deposits: bool = False
    can_manage_loans: bool = False

class RoleCreate(RoleBase):
    pass

class RoleUpdate(BaseModel):
    name: Optional[str] = None
    is_admin: Optional[bool] = None
    can_manage_users: Optional[bool] = None
    can_manage_roles: Optional[bool] = None
    can_manage_branches: Optional[bool] = None
    can_view_settings: Optional[bool] = None
    can_clear_logs: Optional[bool] = None
    can_manage_employees: Optional[bool] = None
    can_view_reports: Optional[bool] = None
    can_manage_pay: Optional[bool] = None
    can_manage_absences: Optional[bool] = None
    can_manage_leaves: Optional[bool] = None
    can_manage_deposits: Optional[bool] = None
    can_manage_loans: Optional[bool] = None

class RoleOut(RoleBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
# --- FIN NOUVEAUX SCHÉMAS ---


# --- Schémas Utilisateur ---
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UserBase(BaseModel):
    email: EmailStr
    full_name: str
    # --- MODIFIÉ ---
    role_id: int
    # --- FIN MODIFIÉ ---
    branch_id: Optional[int] = None
    is_active: bool = True

class UserCreate(UserBase):
    password: str = Field(min_length=6)

class UserOut(UserBase):
    id: int
    # --- AJOUTÉ : Inclure les infos du rôle ---
    role: RoleOut
    # --- FIN AJOUTÉ ---
    model_config = ConfigDict(from_attributes=True)


# --- Schémas Magasin (Branch) ---
class BranchBase(BaseModel):
    name: str
    city: str

class BranchCreate(BranchBase):
    pass

class BranchOut(BranchBase):
    id: int
    model_config = ConfigDict(from_attributes=True)


# --- Schémas Employé ---
class EmployeeBase(BaseModel):
    first_name: str
    last_name: str
    cin: Optional[str] = Field(None, max_length=20)
    position: str
    branch_id: int
    active: bool = True
    
    # --- NOUVEAU CHAMP : Salaire ---
    salary: Optional[Decimal] = Field(None, gt=0, max_digits=10, decimal_places=2)

    @field_validator('cin')
    def validate_cin(cls, v):
        if v is not None and v != "" and not v.isdigit():
            raise ValueError('Le numéro CIN doit être composé uniquement de chiffres.')
        return v
    
    @field_validator('salary')
    def validate_salary(cls, v):
        if v is not None and v < 0:
            raise ValueError('Le salaire doit être un montant positif.')
        return v
    
    model_config = ConfigDict(from_attributes=True)


class EmployeeCreate(EmployeeBase):
    pass

class EmployeeOut(EmployeeBase):
    id: int


# --- Schémas Présence (Attendance) ---
class AttendanceCreate(BaseModel):
    employee_id: int
    date: date
    atype: AttendanceType
    note: Optional[str] = None

class AttendanceOut(BaseModel):
    id: int
    employee_id: int
    date: date
    atype: AttendanceType
    note: Optional[str]
    created_by: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# --- Schémas Congé (Leave) ---
class LeaveCreate(BaseModel):
    employee_id: int
    start_date: date
    end_date: date
    ltype: LeaveType
    
    model_config = ConfigDict(from_attributes=True)

    @field_validator('end_date')
    def validate_end_date(cls, v, info):
        if 'start_date' in info.data and v < info.data['start_date']:
            raise ValueError('La date de fin ne peut pas être antérieure à la date de début.')
        return v


class LeaveOut(BaseModel):
    id: int
    employee_id: int
    start_date: date
    end_date: date
    ltype: LeaveType
    approved: bool
    created_by: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# --- Schémas Avance (Deposit) ---
class DepositBase(BaseModel):
    employee_id: int
    amount: Decimal = Field(..., gt=0, max_digits=10, decimal_places=2)
    date: date
    note: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

class DepositCreate(DepositBase):
    pass

class DepositOut(DepositBase):
    id: int
    created_by: int
    created_at: datetime
    employee: EmployeeOut
    model_config = ConfigDict(from_attributes=True)


# --- NOUVEAUX SCHÉMAS : Paie (Pay) ---
class PayBase(BaseModel):
    employee_id: int
    amount: Decimal = Field(..., gt=0, max_digits=10, decimal_places=2)
    date: date
    pay_type: PayType # Utiliser l'Enum
    note: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

class PayCreate(PayBase):
    pass

class PayOut(PayBase):
    id: int
    created_by: int
    created_at: datetime
    employee: EmployeeOut
# --- FIN DES NOUVEAUX SCHÉMAS ---


# --- Schéma Journal d'Audit ---
class AuditOut(BaseModel):
    id: int
    actor_id: int
    action: str
    entity: str
    entity_id: Optional[int]
    branch_id: Optional[int]
    details: Optional[str]
    created_at: datetime
    
    # --- MODIFIÉ : Ajout du champ pour le nom de l'acteur ---
    actor_full_name: Optional[str] = None
    # --- FIN MODIFICATION ---
    
    model_config = ConfigDict(from_attributes=True)

# --- Loans Schemas ---
class LoanBase(BaseModel):
    employee_id: int
    principal: Decimal = Field(..., gt=0, max_digits=12, decimal_places=3)
    interest_type: Literal["none", "flat", "reducing"] = "none" # <-- AJOUTEZ = "none"
    annual_interest_rate: Decimal | None = Field(None, ge=0, max_digits=7, decimal_places=4)
    term_count: int = Field(..., gt=0, le=480)
    term_unit: Literal["week", "month"]
    start_date: date
    first_due_date: date | None = None
    fee: Decimal | None = Field(None, ge=0, max_digits=10, decimal_places=3)
    notes: str | None = None

class LoanCreate(LoanBase):
    pass

class LoanOut(LoanBase):
    id: int
    status: Literal["draft","approved","active","paid","defaulted","canceled"]
    scheduled_total: Decimal
    repaid_total: Decimal
    outstanding_principal: Decimal
    next_due_on: date | None
    created_by: int
    employee: EmployeeOut | None = None
    model_config = ConfigDict(from_attributes=True)

class LoanScheduleOut(BaseModel):
    id: int
    loan_id: int
    sequence_no: int
    due_date: date
    due_principal: Decimal
    due_interest: Decimal
    due_total: Decimal
    paid_principal: Decimal
    paid_interest: Decimal
    paid_total: Decimal
    paid_on: date | None
    status: Literal["pending","partial","paid","overdue"]
    class Config: from_attributes = True

class RepaymentCreate(BaseModel):
    amount: Decimal = Field(..., gt=0, max_digits=12, decimal_places=3)
    source: Literal["payroll","cash","adjustment"]
    paid_on: date
    schedule_id: int | None = None
    notes: str | None = None

class RepaymentOut(RepaymentCreate):
    id: int
    loan_id: int
    created_by: int
    class Config: from_attributes = True


# --- NOUVEAUX SCHÉMAS : Dépenses (Expenses) ---
class ExpenseBase(BaseModel):
    description: str
    amount: Decimal = Field(..., gt=0, max_digits=10, decimal_places=2)
    date: date
    category: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

class ExpenseCreate(ExpenseBase):
    pass

class ExpenseOut(ExpenseBase):
    id: int
    created_by: int
    created_at: datetime
# --- FIN DES NOUVEAUX SCHÉMAS ---
