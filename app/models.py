""" Modèles ORM de la base de données pour Bijouterie Zaher App. """
from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
    Numeric,
    desc,
    UniqueConstraint  # <--- Fix: Import UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base

# ... (rest of imports/models) ...

# --- SALES SUMMARY MODEL ---
class SalesSummary(Base):
    __tablename__ = "sales_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    
    # "Local User Name" (e.g. "ANISSA") sent from local DB
    local_user_name: Mapped[str] = mapped_column(String(255))
    
    # Store Identifier (e.g. "Ariana" or "Nabeul")
    store_name: Mapped[str] = mapped_column(String(50))

    # Calculated Metrics
    quantity_sold: Mapped[int] = mapped_column(Integer, default=0) # Number of items (lines from detfact)
    total_revenue: Mapped[Decimal] = mapped_column(Numeric(19, 4), default=0.0) # Total Sales Amount

    # Link to Employee (if mapped)
    employee_id: Mapped[int | None] = mapped_column(ForeignKey("employees.id"), nullable=True)
    employee = relationship("Employee", back_populates="sales_summaries")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        # Prevent duplicates for the same user/store/date
        UniqueConstraint('date', 'local_user_name', 'store_name', name='uix_sales_summary'),
    )
class Role(Base):
    __tablename__ = "roles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)

    # --- Permissions ---
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_users: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_roles: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_branches: Mapped[bool] = mapped_column(Boolean, default=False)
    can_view_settings: Mapped[bool] = mapped_column(Boolean, default=False)
    can_clear_logs: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_employees: Mapped[bool] = mapped_column(Boolean, default=False)
    can_view_reports: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_pay: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_absences: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_leaves: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_deposits: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_loans: Mapped[bool] = mapped_column(Boolean, default=False)
    can_manage_expenses: Mapped[bool] = mapped_column(Boolean, default=False) # Nouveau permission

    # Relations
    users = relationship("User", back_populates="permissions")

    def to_dict(self):
        """Renvoie les permissions sous forme de dictionnaire."""
        return {
            "id": self.id,
            "name": self.name,
            "is_admin": self.is_admin,
            "can_manage_users": self.can_manage_users,
            "can_manage_roles": self.can_manage_roles,
            "can_manage_branches": self.can_manage_branches,
            "can_view_settings": self.can_view_settings,
            "can_clear_logs": self.can_clear_logs,
            "can_manage_employees": self.can_manage_employees,
            "can_view_reports": self.can_view_reports,
            "can_manage_pay": self.can_manage_pay,
            "can_manage_absences": self.can_manage_absences,
            "can_manage_leaves": self.can_manage_leaves,
            "can_manage_deposits": self.can_manage_deposits,
            "can_manage_loans": self.can_manage_loans,
            "can_manage_expenses": self.can_manage_expenses,
        }
# --- FIN NOUVEAU MODÈLE ---


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    full_name: Mapped[str] = mapped_column(String(255))
    hashed_password: Mapped[str] = mapped_column(String(255))
    role_id: Mapped[int | None] = mapped_column(ForeignKey("roles.id"))
    branch_id: Mapped[int | None] = mapped_column(ForeignKey("branches.id"), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Relations
    permissions = relationship("Role", back_populates="users", lazy="joined")
    branch = relationship("Branch", back_populates="users")


class Branch(Base):  # Magasin
    __tablename__ = "branches"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    city: Mapped[str] = mapped_column(String(120))
    # Relations
    users = relationship("User", back_populates="branch")
    employees = relationship("Employee", back_populates="branch")


class Employee(Base):
    __tablename__ = "employees"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str] = mapped_column(String(120))
    last_name: Mapped[str] = mapped_column(String(120))
    cin: Mapped[str | None] = mapped_column(String(20), unique=True, nullable=True, index=True)
    position: Mapped[str] = mapped_column(String(120))
    branch_id: Mapped[int] = mapped_column(ForeignKey("branches.id"))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    salary: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    salary_frequency: Mapped[SalaryFrequency] = mapped_column(Enum(SalaryFrequency), default=SalaryFrequency.monthly)
    has_cnss: Mapped[bool] = mapped_column(Boolean, default=False) # Checkbox CNSS

    # Relations
    branch = relationship("Branch", back_populates="employees")
    attendances = relationship("Attendance", back_populates="employee")
    leaves = relationship("Leave", back_populates="employee")
    deposits = relationship("Deposit", back_populates="employee")
    pay_history = relationship("Pay", back_populates="employee")

    # --- AJOUT DE LA RELATION INVERSE ---
    loans = relationship("Loan", back_populates="employee")
    sales_summaries = relationship("SalesSummary", back_populates="employee")
    # --- FIN ---

class SalaryFrequency(str, enum.Enum):
    monthly = "monthly"
    weekly = "weekly"


class AttendanceType(str, enum.Enum):
    present = "present"
    absent = "absent"


class Attendance(Base):
    __tablename__ = "attendance"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"))
    date: Mapped[date] = mapped_column(Date, index=True)
    atype: Mapped[AttendanceType] = mapped_column(Enum(AttendanceType))
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Relations
    employee = relationship("Employee", back_populates="attendances")
    creator = relationship("User")


class LeaveType(str, enum.Enum):
    paid = "paid"
    unpaid = "unpaid"
    sick = "sick"


class Leave(Base):  # Congé
    __tablename__ = "leaves"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"))
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)
    ltype: Mapped[LeaveType] = mapped_column(Enum(LeaveType))
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Relations
    employee = relationship("Employee", back_populates="leaves")
    creator = relationship("User")


class Deposit(Base):  # Avance
    __tablename__ = "deposits"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"))
    amount: Mapped[float] = mapped_column(Numeric(10, 2))
    date: Mapped[date] = mapped_column(Date, index=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Relations
    employee = relationship("Employee", back_populates="deposits")
    creator = relationship("User")


class PayType(str, enum.Enum):
    hebdomadaire = "hebdomadaire"
    mensuel = "mensuel"
    # --- AJOUTÉ ---
    avance = "avance"
    prime_rendement = "prime_rendement"


class Pay(Base):
    __tablename__ = "pay_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"))
    amount: Mapped[float] = mapped_column(Numeric(10, 2))
    date: Mapped[date] = mapped_column(Date, index=True)
    pay_type: Mapped[PayType] = mapped_column(Enum(PayType))
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Relations
    employee = relationship("Employee", back_populates="pay_history")
    creator = relationship("User")


class AuditLog(Base):
    __tablename__ = "audit_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    actor_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    action: Mapped[str] = mapped_column(String(120))
    entity: Mapped[str] = mapped_column(String(120))
    entity_id: Mapped[int | None]
    branch_id: Mapped[int | None] = mapped_column(ForeignKey("branches.id"), nullable=True)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# --- Loans / Advances (structured) ---

class LoanInterestType(str, enum.Enum):
    none = "none"
    flat = "flat"
    reducing = "reducing"


class LoanTermUnit(str, enum.Enum):
    week = "week"
    month = "month"


class LoanStatus(str, enum.Enum):
    draft = "draft"
    approved = "approved"
    active = "active"
    paid = "paid"
    defaulted = "defaulted"
    canceled = "canceled"


class ScheduleStatus(str, enum.Enum):
    pending = "pending"
    partial = "partial"
    paid = "paid"
    overdue = "overdue"


class RepaymentSource(str, enum.Enum):
    payroll = "payroll"
    cash = "cash"
    adjustment = "adjustment"


class Loan(Base):
    __tablename__ = "loans"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"), index=True)
    principal: Mapped[Decimal] = mapped_column(Numeric(12, 3))
    interest_type: Mapped[LoanInterestType] = mapped_column(Enum(LoanInterestType))
    annual_interest_rate: Mapped[Decimal | None] = mapped_column(Numeric(7, 4), nullable=True)
    term_count: Mapped[int] = mapped_column(Integer)
    term_unit: Mapped[LoanTermUnit] = mapped_column(Enum(LoanTermUnit))
    start_date: Mapped[date] = mapped_column(Date)
    first_due_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    fee: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    status: Mapped[LoanStatus] = mapped_column(Enum(LoanStatus), default=LoanStatus.draft)

    scheduled_total: Mapped[Decimal] = mapped_column(Numeric(12, 3), default=Decimal("0"))
    repaid_total: Mapped[Decimal] = mapped_column(Numeric(12, 3), default=Decimal("0"))
    outstanding_principal: Mapped[Decimal] = mapped_column(Numeric(12, 3), default=Decimal("0"))
    next_due_on: Mapped[date | None] = mapped_column(Date, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # --- CORRECTION FINALE AVEC LAMBDA ---
    employee = relationship("Employee", back_populates="loans")
    creator = relationship("User")

    schedules = relationship(
        "LoanSchedule",
        back_populates="loan",
        cascade="all, delete-orphan",
        # Utilisez une lambda pour résoudre la référence APRES la définition de LoanSchedule
        order_by=lambda: LoanSchedule.sequence_no
    )
    repayments = relationship(
        "LoanRepayment",
        back_populates="loan",
        cascade="all, delete-orphan",
        # Utilisez une lambda avec desc() pour résoudre la référence
        order_by=lambda: desc(LoanRepayment.paid_on)
    )
    # --- FIN CORRECTION ---


class LoanSchedule(Base):
    __tablename__ = "loan_schedules"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    loan_id: Mapped[int] = mapped_column(ForeignKey("loans.id"), index=True)
    sequence_no: Mapped[int] = mapped_column(Integer, index=True)
    due_date: Mapped[date] = mapped_column(Date, index=True)

    due_principal: Mapped[Decimal] = mapped_column(Numeric(12, 3))
    due_interest: Mapped[Decimal] = mapped_column(Numeric(12, 3))
    due_total: Mapped[Decimal] = mapped_column(Numeric(12, 3))

    paid_principal: Mapped[Decimal] = mapped_column(Numeric(12, 3), default=Decimal("0"))
    paid_interest: Mapped[Decimal] = mapped_column(Numeric(12, 3), default=Decimal("0"))
    paid_total: Mapped[Decimal] = mapped_column(Numeric(12, 3), default=Decimal("0"))
    paid_on: Mapped[date | None] = mapped_column(Date, nullable=True)

    status: Mapped[ScheduleStatus] = mapped_column(Enum(ScheduleStatus), default=ScheduleStatus.pending)

    loan = relationship("Loan", back_populates="schedules")


class LoanRepayment(Base):
    __tablename__ = "loan_repayments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    loan_id: Mapped[int] = mapped_column(ForeignKey("loans.id"), index=True)
    schedule_id: Mapped[int | None] = mapped_column(ForeignKey("loan_schedules.id"), nullable=True, index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 3))
    source: Mapped[RepaymentSource] = mapped_column(Enum(RepaymentSource))
    paid_on: Mapped[date] = mapped_column(Date, index=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    loan = relationship("Loan", back_populates="repayments")
    schedule = relationship("LoanSchedule", foreign_keys=[schedule_id])  # Spécifiez la clé étrangère pour éviter l'ambiguïté
    creator = relationship("User")


class LoanSettings(Base):
    __tablename__ = "loan_settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    max_dti: Mapped[Decimal] = mapped_column(Numeric(5, 3), default=Decimal("0.300"))
    max_concurrent_loans: Mapped[int] = mapped_column(Integer, default=1)
    default_term_unit: Mapped[LoanTermUnit] = mapped_column(Enum(LoanTermUnit), default=LoanTermUnit.month)
    grace_days: Mapped[int] = mapped_column(Integer, default=3)
    penalty_rate_per_period: Mapped[Decimal] = mapped_column(Numeric(6, 4), default=Decimal("0.000"))


class Expense(Base):
    __tablename__ = "expenses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(String(255))
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    date: Mapped[date] = mapped_column(Date, index=True)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    # --- ADDED: Branch Link ---
    branch_id: Mapped[int | None] = mapped_column(ForeignKey("branches.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    
    creator = relationship("User")
    branch = relationship("Branch")


