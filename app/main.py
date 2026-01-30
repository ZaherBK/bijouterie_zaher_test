import os
from datetime import timedelta, date as dt_date, datetime
from decimal import Decimal
from typing import Annotated, List, Optional
import json
import enum # Ajout de l'import enum manquant
import traceback # Pour un meilleur logging d'erreur
import pytz

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request # Ensure this is imported
# --- MODIFIÉ : Ajout de coalesce ---
from sqlalchemy import select, update, delete, func, and_, or_, desc, asc, text, case, extract
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from . import models, schemas # Keep this general import if other parts of the file use models.XXX
import io # Importé pour l'export

# --- CORRIGÉ : Import de get_db depuis .deps ---
from .db import engine, Base, AsyncSessionLocal
# --- CORRIGÉ : Import de hash_password ---
from .auth import authenticate_user, hash_password, ACCESS_TOKEN_EXPIRE_MINUTES, api_require_permission

# Importer TOUS les modèles nécessaires (including Role and Enums explicitly)
from .models import (
    Role, PayType, AttendanceType, LeaveType, LoanStatus, LoanTermUnit, ScheduleStatus,
    RepaymentSource, User, Branch, Employee, Attendance, Leave, Deposit, Pay, Loan,
    LoanSchedule, LoanRepayment, AuditLog, LoanInterestType, SalesSummary, SalaryFrequency # Added SalesSummary and SalaryFrequency
)
# Import Schemas needed in main.py
from .schemas import RoleCreate, RoleUpdate, LoanCreate, RepaymentCreate

# --- FIX: Import audit functions ---
from .audit import latest, log
# --- END FIX ---

# Import Routers
from .routers import users, branches, employees as employees_api, attendance as attendance_api, leaves as leaves_api, deposits as deposits_api, sales
from .routers import pay
# --- MODIFIÉ : Importer les nouvelles dépendances ---
from .deps import get_db, web_require_permission
# --- NOUVEAU: Import de la fonction safe si elle est dans deps.py ---
from .deps import get_user_data_from_session_safe
# --- FIN NOUVEAU ---
# --- LOANS API Router ---
from app.api import loans as loans_api
# Note: Redundant imports like `from app.models import Employee...` are removed as they are covered by `from .models import ...`

# --- FIN MODIFIÉ ---

APP_NAME = os.getenv("APP_NAME", "Bijouterie Zaher")

# --- NEW: Define Tunisia Timezone (UTC+1) ---

TUNISIA_TZ = pytz.timezone("Africa/Tunis")

# --- FIN MODIFIÉ ---

# --- MIGRATIONS ---
from contextlib import asynccontextmanager
from .migrations import run_migrations

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Run migrations
    await run_migrations()
    yield
    # Shutdown
    pass

app = FastAPI(
    title=APP_NAME,
    lifespan=lifespan
)

@app.get("/api/health")
def health_check():
    """Lightweight endpoint for keep-alive pings."""
    return {"status": "ok"}

@app.get("/fix-migration")
async def fix_migration_manual(db: AsyncSession = Depends(get_db)):
    """Manual trigger to fix database missing column and backfill data."""
    messages = []
    try:
        # 1. Add Column
        try:
            await db.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS branch_id INTEGER REFERENCES branches(id)"))
            await db.commit()
            messages.append("Column 'branch_id' checked/added.")
        except Exception as e:
            messages.append(f"Column check skipped/failed: {e}")

        # 2. Backfill Data (Fix NULLs)
        # Update expenses set branch_id from the creator's branch
        # PostgreSQL syntax
        try:
            await db.execute(text("""
                UPDATE expenses 
                SET branch_id = users.branch_id 
                FROM users 
                WHERE expenses.created_by = users.id 
                AND expenses.branch_id IS NULL
            """))
            await db.commit()
            messages.append("Data Backfill successful: Assigned branches to existing expenses.")
        except Exception as e:
            messages.append(f"Data Backfill failed: {e}")

        return {"status": "success", "messages": messages}
    except Exception as e:
        return {"status": "error", "message": f"Global error: {str(e)}"}

@app.get("/fix-salary-column")
async def fix_salary_column(db: AsyncSession = Depends(get_db)):
    """Migration manuelle pour ajouter la colonne salary_frequency."""
    messages = []
    try:
        # 1. Add Column salary_frequency (ENUM or VARCHAR)
        # PostgreSQL syntax
        try:
            # Check if type exists first (for Enum)
            # But simpler to just use VARCHAR for safety or let SQL Alchemy handle it? 
            # No, raw SQL is safer for migration.
            # We'll use VARCHAR(50) to store 'monthly'/'weekly' to avoid Enum complications in raw SQL.
            # OR we can try to create the TYPE.
            
            # Let's try adding column as VARCHAR first, or TEXT check constraint.
            # Actually, let's just use Text/Varchar and let SQLAlchemy cast it later, 
            # or try to create the enum type.
            
            await db.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS salary_frequency VARCHAR(50) DEFAULT 'monthly'"))
            await db.commit()
            messages.append("Colone 'salary_frequency' ajoutée.")
        except Exception as e:
            messages.append(f"Erreur ajout colonne: {e}")

        return {"status": "success", "messages": messages}
    except Exception as e:
        return {"status": "error", "message": f"Global error: {str(e)}"}
app.include_router(users.router)
app.include_router(branches.router)
app.include_router(employees_api.router)
app.include_router(attendance_api.router)
app.include_router(leaves_api.router)
app.include_router(deposits_api.router)
app.include_router(loans_api.router)
# --- NOUVEAU: Expenses Router ---
from .routers import expenses, sync
app.include_router(expenses.router)
app.include_router(sync.router)
app.include_router(pay.router)
app.include_router(sales.router)
# --- FIN NOUVEAU ---
# --- 2. Static/Templates Setup ---
BASE_DIR = os.path.dirname(__file__)
static_path = os.path.join(BASE_DIR, "frontend", "static")
templates_path = os.path.join(BASE_DIR, "frontend", "templates")

os.makedirs(static_path, exist_ok=True)
os.makedirs(templates_path, exist_ok=True)

app.mount(
    "/static",
    StaticFiles(directory=static_path),
    name="static",
)
templates = Jinja2Templates(directory=templates_path)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SECRET_KEY", "une_cle_secrete_tres_longue_et_aleatoire"),
    max_age=int(ACCESS_TOKEN_EXPIRE_MINUTES) * 60
)

@app.get("/branches", response_class=HTMLResponse, name="branches_page")
async def branches_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_branches"))
):
    """Page de gestion des magasins (Admin)."""
    res = await db.execute(select(models.Branch).order_by(models.Branch.name))
    
    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "branches": res.scalars().all(),
    }
    return templates.TemplateResponse("branches.html", context)


# --- 2. Static/Templates Setup ---

def format_datetime_tunisia(dt: datetime | None):
    """Converts a UTC datetime object to 'Africa/Tunis' string format."""
    if dt is None:
        return "" # Return an empty string if the date is null
    
    # 1. If the datetime is "naive" (no timezone), assume it's UTC
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    
    # 2. Convert the (now timezone-aware) time to Tunisia's timezone
    tunisia_time = dt.astimezone(TUNISIA_TZ)
    
    # 3. Format it as a clean string
    return tunisia_time.strftime('%Y-%m-%d %H:%M:%S')

# 4. Register the function as a filter in your templates
templates.env.filters['to_tunisia'] = format_datetime_tunisia
# --- END OF ADDED CODE ---

# Helper function for dynamic, timezone-aware date
def get_tunisia_today():
    return datetime.now(TUNISIA_TZ).date()

# 1. Create a NEW dependency to get the FULL database user
async def get_current_db_user(
    db: AsyncSession = Depends(get_db),
    # MODIFIÉ: Utiliser la version 'safe' qui retourne None au lieu de rediriger
    user_data: dict | None = Depends(get_user_data_from_session_safe)
) -> models.User | None:

    if not user_data:
        return None

    user_email = user_data.get("email")
    if not user_email:
        return None

    result = await db.execute(
        select(models.User)
        .options(
            selectinload(models.User.permissions),
            selectinload(models.User.branch)
        )
        .where(models.User.email == user_email)
    )
    return result.scalar_one_or_none()


# --- 3. Startup Event (MODIFIÉ) ---
# ... (Startup code remains the same - not shown for brevity) ...
@app.on_event("startup")
async def on_startup() -> None:
    """Créer les tables de la base de données et ajouter les rôles/données initiaux."""
    print("Événement de démarrage...")
    async with engine.begin() as conn:
        print("Création de toutes les tables (si elles n'existent pas)...")
        await conn.run_sync(Base.metadata.create_all)
        print("Tables OK.")

    try:
        async with AsyncSessionLocal() as session:
            # --- EMERGENCY MIGRATION AT STARTUP ---
            try:
                print("Checking DB Schema for missing columns...")
                await session.execute(text("ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_manage_expenses BOOLEAN DEFAULT FALSE;"))
                await session.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS has_cnss BOOLEAN DEFAULT FALSE;"))
                await session.commit()
                print("DB Schema verified.")
            except Exception as e_mig:
                print(f"Migration step warning: {e_mig}")
                await session.rollback()
            # --------------------------------------

            # --- PASSWORD RESET (USER REQUEST) ---
            try:
                print("Resetting passwords for Admin/BK ZAHER...")
                new_pwd = hash_password("5")
                # 1. Update standard Admin email
                await session.execute(update(User).where(User.email == "zaher@local").values(hashed_password=new_pwd))
                # 2. Update specific User 'BK ZAHER' if exists (and distinct)
                await session.execute(update(User).where(User.full_name == "BK ZAHER").values(hashed_password=new_pwd))
                await session.commit()
            except Exception as e_pwd:
                print(f"Password reset warning: {e_pwd}")
            # -------------------------------------

            res_admin_role = await session.execute(select(Role).where(Role.name == "Admin"))
            admin_role = res_admin_role.scalar_one_or_none()

            if not admin_role:
                print("Base de données vide, ajout des rôles et utilisateurs initiaux (seed)...")

                admin_role = Role(
                    name="Admin", is_admin=True, can_manage_users=True, can_manage_roles=True,
                    can_manage_branches=True, can_view_settings=True, can_clear_logs=True,
                    can_manage_employees=True, can_view_reports=True, can_manage_pay=True,
                    can_manage_absences=True, can_manage_leaves=True, can_manage_deposits=True,
                    can_manage_loans=True, can_manage_expenses=True
                )
                manager_role = Role(
                    name="Manager", is_admin=False, can_manage_users=False, can_manage_roles=False,
                    can_manage_branches=False, can_view_settings=False, can_clear_logs=False,
                    can_manage_employees=True, can_view_reports=False, can_manage_pay=True,
                    can_manage_absences=True, can_manage_leaves=True, can_manage_deposits=True,
                    can_manage_loans=True
                )
                session.add_all([admin_role, manager_role])
                await session.flush() # Assigner les IDs aux roles

                # Créer les branches même si on ne crée pas les managers par défaut
                res_branch = await session.execute(select(Branch).where(Branch.name == "Magasin Ariana"))
                branch_ariana = res_branch.scalar_one_or_none()

                if not branch_ariana:
                    print("Ajout des magasins par défaut...")
                    branch_ariana = Branch(name="Magasin Ariana", city="Ariana")
                    branch_nabeul = Branch(name="Magasin Nabeul", city="Nabeul")
                    session.add_all([branch_ariana, branch_nabeul])
                    await session.flush() # Assigner les IDs aux branches
                # Pas besoin de else ici, si elles existent déjà, c'est bon.

                res_admin_user = await session.execute(select(User).where(User.email == "zaher@local"))

                if res_admin_user.scalar_one_or_none() is None:
                    print("Ajout de l'utilisateur admin initial...")
                    # --- FIX: Créer seulement l'utilisateur Admin ---
                    admin_user = User(
                            email="zaher@local", full_name="Zaher (Admin)", role_id=admin_role.id,
                            hashed_password=hash_password("5"), is_active=True, branch_id=None
                        )
                    session.add(admin_user)
                    # --- FIN DU FIX ---
                    await session.commit()
                    print(f"✅ Rôles, Magasins et l'utilisateur Admin créés avec succès !")
                else:
                    print("Utilisateur admin déjà présent, commit des rôles/magasins si nécessaire.")
                    await session.commit() # Commit au cas où les roles/branches ont été créés
            else:
                print("Données initiales déjà présentes. Seeding ignoré.")
    except Exception as e:
        print(f"Erreur pendant le seeding initial : {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        await session.rollback()


# --- 4. Fonctions d'aide (Helper Functions) ---
# ... (Functions _serialize_permissions, CustomJSONEncoder, _parse_dates remain the same - not shown for brevity) ...
def _serialize_permissions(role: Role | None) -> dict:
    """Convertit un objet Role en un dictionnaire de permissions pour la session."""
    if not role:
        return {}
    return role.to_dict()

# --- NOUVEAU : Helper pour l'export JSON ---
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (dt_date, datetime)):
            return obj.isoformat()
        # --- FIX: Ne plus exclure hashed_password ---
        if isinstance(obj, Base): # Gérer les objets SQLAlchemy
             return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
        # --- FIN FIX ---
        if isinstance(obj, enum.Enum):
            return obj.value
        return super().default(obj)
# --- FIN NOUVEAU ---

# --- NOUVEAU: Helper pour convertir les dates/datetimes lors de l'import ---
def _parse_dates(
    item: dict,
    date_fields: Optional[list[str]] = None,
    datetime_fields: Optional[list[str]] = None,
):
    """Convertit les champs date/datetime string d'un dict en objets Python."""
    date_fields = date_fields or []
    datetime_fields = datetime_fields or []
    
    for field in date_fields:
        if field in item and item[field] and isinstance(item[field], str): # Ajout de 'item[field]' pour vérifier non-None
            try:
                item[field] = dt_date.fromisoformat(item[field])
            except ValueError:
                print(f"AVERTISSEMENT: Impossible de parser la date '{item[field]}' pour le champ '{field}'. Mise à None.")
                item[field] = None
    for field in datetime_fields:
        if field in item and item[field] and isinstance(item[field], str): # Ajout de 'item[field]' pour vérifier non-None
            try:
                # Gérer les différents formats possibles (avec/sans T, avec/sans Z/+offset)
                dt_str = item[field].replace('T', ' ').split('.')[0] # Enlever les millisecondes
                dt_str = dt_str.split('+')[0].split('Z')[0].strip() # Enlever offset/Z

                # Essayer différents formats si fromisoformat échoue
                try:
                    item[field] = datetime.fromisoformat(dt_str)
                except ValueError:
                    # Tenter avec un format commun si isoformat échoue (ex: backup ancien)
                    item[field] = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print(f"AVERTISSEMENT: Impossible de parser datetime '{item[field]}' pour le champ '{field}'. Mise à None.")
                item[field] = None
    return item


# --- 5. Routes des Pages Web (GET et POST) ---

# MODIFIÉ: S'assurer que current_user est Optional[models.User] pour que l'opération 'if not current_user' fonctionne
@app.get("/", response_class=HTMLResponse, name="home")
async def home(
    request: Request,
    db: AsyncSession = Depends(get_db), # <<< Add db dependency
    current_user: Optional[models.User] = Depends(get_current_db_user) # Get full user object (now Optional)
):
    # CETTE LIGNE EST MAINTENANT LA SEULE SOURCE DE REDIRECTION POUR LA PAGE D'ACCUEIL
    if current_user is None:
        return RedirectResponse(request.url_for("login_page"), status_code=status.HTTP_302_FOUND)

# --- FIX: Fetch recent activity logs FOR ADMIN ---
    # Le reste de la logique reste le même car il se trouve maintenant dans le bloc
    # qui n'est exécuté que si current_user est valide.
    activity_logs = []
    # Ensure permissions relation is loaded and user has permissions attribute
    if hasattr(current_user, 'permissions') and current_user.permissions and current_user.permissions.is_admin:
        permissions_dict = current_user.permissions.to_dict() # Use the existing method
        # --- FIX: Call 'latest' correctly ---
        activity_logs = await latest(
            db, # Pass db as the first argument
            user_is_admin=permissions_dict.get("is_admin", False),
            branch_id=current_user.branch_id, # Use branch_id from the full user object
             # Fetch a broader range of activities for the admin dashboard view
            entity_types=["leave", "attendance", "deposit", "pay", "loan", "expense", "user", "role", "employee", "branch", "all_logs"],
            limit=50 # Increased activity limit to 50 per user request
        )
        # --- END FIX ---
        # Optional Eager Loading (commented out as 'latest' might handle it)
        # actor_ids = {log.actor_id for log in activity_logs if log.actor_id}
        # if actor_ids:
        #     actors_res = await db.execute(select(User).where(User.id.in_(actor_ids)))
        #     actors_map = {actor.id: actor for actor in actors_res.scalars()}
        #     for log in activity_logs:
        #         log.actor = actors_map.get(log.actor_id)
    # --- END FIX BLOCK ---

    context = {
        "request": request,
        "user": current_user, # Pass the full user object
        "activity": activity_logs # Pass activity logs to template
    }
    return templates.TemplateResponse("dashboard.html", context)


@app.get("/login", response_class=HTMLResponse, name="login_page")
async def login_page(request: Request, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(User).order_by(User.full_name))
    users = res.scalars().all()
    return templates.TemplateResponse("login.html", {"request": request, "app_name": APP_NAME, "users": users})


@app.post("/login", name="login_action")
async def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_db)
):
    user = await authenticate_user(db, username, password)

    if not user:
        # --- FIX: Re-fetch users list on failed login ---
        res_users = await db.execute(select(User).order_by(User.full_name))
        users_list = res_users.scalars().all()
        # --- END FIX ---
        context = {
            "request": request,
            "app_name": APP_NAME,
            "error": "Email ou mot de passe incorrect.",
            "users": users_list # --- FIX: Pass users list to context ---
        }
        return templates.TemplateResponse("login.html", context, status_code=status.HTTP_401_UNAUTHORIZED)

    # If login is successful
    permissions_dict = _serialize_permissions(user.permissions)

    request.session["user"] = {
        "email": user.email,
        "id": user.id,
        "full_name": user.full_name,
        "branch_id": user.branch_id,
        "permissions": permissions_dict
    }

    return RedirectResponse(request.url_for('home'), status_code=status.HTTP_302_FOUND)


@app.get("/logout", name="logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(request.url_for('login_page'), status_code=status.HTTP_302_FOUND)


# --- Employés ---
# ... (Employees routes remain the same - not shown for brevity) ...
@app.get("/employees", response_class=HTMLResponse, name="employees_page")
async def employees_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_employees"))
):
    branches_query = select(Branch)
    employees_query = select(Employee).where(Employee.active == True).order_by(Employee.first_name)

    manager_branch_id = None
    permissions = user.get("permissions", {})

    if not permissions.get("is_admin"):
        manager_branch_id = user.get("branch_id")
        branches_query = branches_query.where(Branch.id == manager_branch_id)
        employees_query = employees_query.where(Employee.branch_id == manager_branch_id)
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             manager_branch_id = int(branch_filter_id)
             employees_query = employees_query.where(Employee.branch_id == manager_branch_id)

    res_branches = await db.execute(branches_query)
    res_employees = await db.execute(employees_query)

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": res_employees.scalars().all(),
        "branches": res_branches.scalars().all(),
        "manager_branch_id": manager_branch_id,
        "selected_branch_id": request.query_params.get("branch_id") # FIX: Pass for Admin UI state
    }
    return templates.TemplateResponse("employees.html", context)


@app.post("/employees/create", name="employees_create")
async def employees_create(
    request: Request,
    first_name: Annotated[str, Form()],
    last_name: Annotated[str, Form()],
    position: Annotated[str, Form()],
    db: AsyncSession = Depends(get_db), # Restored missing db dependency
    user: dict = Depends(web_require_permission("can_manage_employees")),
    branch_id: Annotated[int | None, Form()] = None, # Make optional, fill later if missing
    cin: Annotated[str, Form()] = None,
    salary: Annotated[Decimal, Form()] = None,
    has_cnss: bool = Form(False)
):
    permissions = user.get("permissions", {})
    
    # Logic: If manager, Force branch_id. If Admin, Require branch_id.
    if not permissions.get("is_admin"):
        # Manager/Gerant: Force their branch DB
        branch_id = user.get("branch_id")
    else:
        # Admin: Must select a branch
        if not branch_id:
             # Should show error, but for now redirect
             print("ERREUR: Admin n'a pas sélectionné de magasin.")
             return RedirectResponse(request.url_for('employees_page'), status_code=status.HTTP_302_FOUND)

    # Was: if not permissions.get("is_admin"): salary = None -> REMOVED to allow salary setting


    if cin:
        res_cin = await db.execute(select(Employee).where(Employee.cin == cin))
        if res_cin.scalar_one_or_none():
            return RedirectResponse(request.url_for('employees_page'), status_code=status.HTTP_302_FOUND)

    new_employee = Employee(
        first_name=first_name, last_name=last_name, cin=cin or None,
        position=position, branch_id=branch_id, salary=salary, active=True,
        has_cnss=has_cnss
    )
    db.add(new_employee)
    await db.commit()
    await db.refresh(new_employee)

    await log(
        db, user['id'], "create", "employee", new_employee.id,
        new_employee.branch_id, f"Employé créé: {first_name} {last_name}"
    )

    return RedirectResponse(request.url_for('employees_page'), status_code=status.HTTP_302_FOUND)


@app.post("/employees/{employee_id}/update", name="employees_update")
async def employees_update(
    request: Request,
    employee_id: int,
    first_name: Annotated[str, Form()],
    last_name: Annotated[str, Form()],
    position: Annotated[str, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_employees")),
    branch_id: Annotated[int | None, Form()] = None,
    cin: Annotated[str, Form()] = None,
    salary: Annotated[Decimal, Form()] = None,
    has_cnss: bool = Form(False),
    salary_frequency: Annotated[SalaryFrequency, Form()] = SalaryFrequency.monthly # <-- NOUVEAU
):
    # Fetch Employee
    res_emp = await db.execute(select(Employee).where(Employee.id == employee_id))
    employee = res_emp.scalar_one_or_none()
    
    if not employee:
        return RedirectResponse(request.url_for('employees_page'), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    
    # Permission Check
    if not permissions.get("is_admin"):
        if user.get("branch_id") != employee.branch_id:
             return RedirectResponse(request.url_for('employees_page'), status_code=status.HTTP_403_FORBIDDEN)
        branch_id = employee.branch_id
    else:
        if not branch_id:
            branch_id = employee.branch_id

    # Update Fields
    employee.first_name = first_name
    employee.last_name = last_name
    employee.cin = cin or None
    employee.position = position
    employee.salary = salary
    employee.branch_id = branch_id
    employee.has_cnss = has_cnss
    employee.salary_frequency = salary_frequency # <-- UPDATE

    await db.commit()
    await db.refresh(employee)

    await log(
        db, user['id'], "update", "employee", employee.id,
        employee.branch_id, f"Employé modifié: {first_name} {last_name}"
    )

    return RedirectResponse(request.url_for('employees_page'), status_code=status.HTTP_302_FOUND)


# --- Absences (Attendance) ---
# ... (Attendance routes remain the same - not shown for brevity) ...
@app.get("/attendance", response_class=HTMLResponse, name="attendance_page")
async def attendance_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_absences"))
):
    employees_query = select(Employee).where(Employee.active == True).order_by(Employee.first_name)
    attendance_query = select(Attendance).options(selectinload(Attendance.employee), selectinload(Attendance.creator)).order_by(Attendance.date.desc(), Attendance.created_at.desc()) # Charger l'employé et le créateur

    permissions = user.get("permissions", {})
    
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch))
    all_branches = res_branches.scalars().all()

    if not permissions.get("is_admin"):
        branch_id = user.get("branch_id")
        employees_query = employees_query.where(Employee.branch_id == branch_id)
        attendance_query = attendance_query.join(Employee).where(Employee.branch_id == branch_id)
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             bid = int(branch_filter_id)
             employees_query = employees_query.where(Employee.branch_id == bid)
             attendance_query = attendance_query.join(Employee).where(Employee.branch_id == bid)

    res_employees = await db.execute(employees_query)
    res_attendance = await db.execute(attendance_query.limit(100))

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": res_employees.scalars().all(),
        "attendance": res_attendance.scalars().all(),
        "branches": all_branches, # Passed for Admin Selector
        "selected_branch_id": request.query_params.get("branch_id"), 
        "today_date": get_tunisia_today().isoformat()
    }
    return templates.TemplateResponse("attendance.html", context)


@app.post("/attendance/create", name="attendance_create")
async def attendance_create(
    request: Request,
    employee_id: Annotated[int, Form()],
    date: Annotated[dt_date, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_absences")),
    note: Annotated[str, Form()] = None
):
    res_emp = await db.execute(select(Employee).where(Employee.id == employee_id))
    employee = res_emp.scalar_one_or_none()
    if not employee:
        return RedirectResponse(request.url_for('attendance_page'), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin") and user.get("branch_id") != employee.branch_id:
        return RedirectResponse(request.url_for('attendance_page'), status_code=status.HTTP_302_FOUND)

    new_attendance = Attendance(
        employee_id=employee_id, date=date, atype=AttendanceType.absent,
        note=note or None, created_by=user['id']
    )
    db.add(new_attendance)
    await db.commit()
    await db.refresh(new_attendance)

    await log(
        db, user['id'], "create", "attendance", new_attendance.id,
        employee.branch_id, f"Absence pour Employé ID={employee_id}, Date={date}"
    )

    return RedirectResponse(request.url_for('attendance_page'), status_code=status.HTTP_302_FOUND)

@app.post("/attendance/{attendance_id}/delete", name="attendance_delete")
async def attendance_delete(
    request: Request,
    attendance_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_absences")) # Ensure correct permission
):
    """Supprime un enregistrement d'absence."""

    # Fetch the attendance record along with the employee to check branch permission
    attendance_query = select(Attendance).options(selectinload(Attendance.employee)).where(Attendance.id == attendance_id)
    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        # Non-admin can only delete if the employee belongs to their branch
        attendance_query = attendance_query.join(Employee).where(Employee.branch_id == user.get("branch_id"))

    res_att = await db.execute(attendance_query)
    attendance_to_delete = res_att.scalar_one_or_none()

    if attendance_to_delete:
        try:
            employee_name = f"{attendance_to_delete.employee.first_name} {attendance_to_delete.employee.last_name}" if attendance_to_delete.employee else f"ID {attendance_to_delete.employee_id}"
            attendance_date = attendance_to_delete.date
            emp_branch_id = attendance_to_delete.employee.branch_id if attendance_to_delete.employee else None

            await db.delete(attendance_to_delete)
            await db.commit()

            # Log the deletion
            await log(
                db, user['id'], "delete", "attendance", attendance_id,
                emp_branch_id, f"Absence supprimée pour {employee_name} le {attendance_date}"
            )
            await db.commit() # Commit the log entry

            print(f"✅ Absence ID={attendance_id} supprimée avec succès.")

        except Exception as e:
            await db.rollback()
            print(f"ERREUR lors de la suppression de l'absence ID={attendance_id}: {e}")
            traceback.print_exc()
            # Optionally add a flash message here

    else:
        # Attendance record not found or user doesn't have permission
        print(f"Tentative de suppression de l'absence ID={attendance_id} échouée (non trouvée ou accès refusé).")

    # Redirect back to the attendance list page
    return RedirectResponse(request.url_for("attendance_page"), status_code=status.HTTP_302_FOUND)
    
# --- Avances (Deposits) ---
# ... (Deposits routes remain the same - not shown for brevity) ...
@app.get("/deposits", response_class=HTMLResponse, name="deposits_page")
async def deposits_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_deposits"))
):
    # Query for employees (for the select dropdown)
    q_emp = select(Employee).where(Employee.active == True).order_by(Employee.first_name)
    
    # Query for Deposits
    q_dep = select(Deposit).options(selectinload(Deposit.employee), selectinload(Deposit.creator)).order_by(Deposit.date.desc(), Deposit.created_at.desc())

    permissions = user.get("permissions", {})
    
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch))
    all_branches = res_branches.scalars().all()

    if not permissions.get("is_admin"):
        # Manager Filter
        branch_id = user.get("branch_id")
        q_emp = q_emp.where(Employee.branch_id == branch_id)
        # Deposits filtered by Employee's branch
        q_dep = q_dep.join(Employee).where(Employee.branch_id == branch_id)
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             bid = int(branch_filter_id)
             q_emp = q_emp.where(Employee.branch_id == bid)
             q_dep = q_dep.join(Employee).where(Employee.branch_id == bid)

    res_emp = await db.execute(q_emp)
    res_dep = await db.execute(q_dep.limit(100))

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": res_emp.scalars().all(),
        "deposits": res_dep.scalars().all(),
        "branches": all_branches, # Passed for Admin Selector
        "selected_branch_id": request.query_params.get("branch_id"), 
        "today_date": get_tunisia_today().isoformat()
    }
    return templates.TemplateResponse("deposits.html", context)


@app.post("/deposits/create", name="deposits_create")
async def deposits_create(
    request: Request,
    employee_id: Annotated[int, Form()],
    amount: Annotated[Decimal, Form()],
    date: Annotated[dt_date, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_deposits")),
    note: Annotated[str, Form()] = None
):
    res_emp = await db.execute(select(Employee).where(Employee.id == employee_id))
    employee = res_emp.scalar_one_or_none()
    if not employee or amount <= 0:
        return RedirectResponse(request.url_for('deposits_page'), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin") and user.get("branch_id") != employee.branch_id:
        return RedirectResponse(request.url_for('deposits_page'), status_code=status.HTTP_302_FOUND)

    new_deposit = Deposit(
        employee_id=employee_id, amount=amount, date=date,
        note=note or None, created_by=user['id']
    )
    db.add(new_deposit)
    await db.commit()
    await db.refresh(new_deposit)

    await log(
        db, user['id'], "create", "deposit", new_deposit.id,
        employee.branch_id, f"Avance pour Employé ID={employee_id}, Montant={amount}"
    )

    return RedirectResponse(request.url_for('deposits_page'), status_code=status.HTTP_302_FOUND)

@app.post("/deposits/{deposit_id}/delete", name="deposits_delete")
async def deposits_delete(
    request: Request,
    deposit_id: int,
    db: AsyncSession = Depends(get_db),
    # --- FIX: Use correct permission 'can_manage_deposits' or is_admin ---
    user: dict = Depends(web_require_permission("can_manage_deposits"))
):
    """Supprime un enregistrement d'avance."""

    # Fetch the deposit record along with the employee to check branch permission
    deposit_query = select(Deposit).options(selectinload(Deposit.employee)).where(Deposit.id == deposit_id)
    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        # Non-admin requires specific permission AND matching branch
        if not permissions.get("can_manage_deposits"): # Double check permission needed
             return RedirectResponse(request.url_for("deposits_page"), status_code=status.HTTP_403_FORBIDDEN)
        deposit_query = deposit_query.join(Employee).where(Employee.branch_id == user.get("branch_id"))

    res_dep = await db.execute(deposit_query)
    deposit_to_delete = res_dep.scalar_one_or_none()

    if deposit_to_delete:
        try:
            employee_name = f"{deposit_to_delete.employee.first_name} {deposit_to_delete.employee.last_name}" if deposit_to_delete.employee else f"ID {deposit_to_delete.employee_id}"
            deposit_date = deposit_to_delete.date
            deposit_amount = deposit_to_delete.amount
            emp_branch_id = deposit_to_delete.employee.branch_id if deposit_to_delete.employee else None

            await db.delete(deposit_to_delete)
            await db.commit()

            # Log the deletion
            await log(
                db, user['id'], "delete", "deposit", deposit_id,
                emp_branch_id, f"Avance supprimée ({deposit_amount} TND) pour {employee_name} du {deposit_date}"
            )
            await db.commit() # Commit the log entry

            print(f"✅ Avance ID={deposit_id} supprimée avec succès.")

        except Exception as e:
            await db.rollback()
            print(f"ERREUR lors de la suppression de l'avance ID={deposit_id}: {e}")
            traceback.print_exc()
            # Optionally add a flash message here

    else:
        # Deposit record not found or user doesn't have permission
        print(f"Tentative de suppression de l'avance ID={deposit_id} échouée (non trouvée ou accès refusé).")

    # Redirect back to the deposits list page
    return RedirectResponse(request.url_for("deposits_page"), status_code=status.HTTP_302_FOUND)

# --- Dépenses (Expenses) - Page Web ---
@app.get("/expenses", response_class=HTMLResponse, name="expenses_page")
async def expenses_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_expenses"))
):
    # Fetch expenses
    expenses_query = select(models.Expense).options(selectinload(models.Expense.creator)).order_by(models.Expense.date.desc(), models.Expense.created_at.desc())

    permissions = user.get("permissions", {})
    
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch))
    all_branches = res_branches.scalars().all()

    if not permissions.get("is_admin"):
        branch_id = user.get("branch_id")
        # Filter expenses created by users in the same branch OR linked to branch (Legacy Support)
        # Logic: (Expense.branch_id == branch_id) OR (Expense.branch_id IS NULL AND Creator.branch_id == branch_id)
        expenses_query = expenses_query.outerjoin(models.User, models.Expense.created_by == models.User.id)
        expenses_query = expenses_query.where(
            or_(
                models.Expense.branch_id == branch_id,
                and_(models.Expense.branch_id.is_(None), models.User.branch_id == branch_id)
            )
        )
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             expenses_query = expenses_query.where(models.Expense.branch_id == int(branch_filter_id))

    res_expenses = await db.execute(expenses_query.limit(100))
    
    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "expenses": res_expenses.scalars().all(),
        "branches": all_branches, # Passed for Admin Selector
        "selected_branch_id": request.query_params.get("branch_id"), 
        "today_date": get_tunisia_today().isoformat()
    }
    return templates.TemplateResponse("expenses.html", context)

@app.post("/expenses/create", name="expenses_create")
async def expenses_create(
    request: Request,
    description: Annotated[str, Form()],
    amount: Annotated[Decimal, Form()],
    date: Annotated[dt_date, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_expenses")),
    branch_id: Annotated[int | None, Form()] = None # Added branch_id
):
    if amount <= 0:
        return RedirectResponse(request.url_for('expenses_page'), status_code=status.HTTP_302_FOUND)

    target_branch_id = branch_id
    if not user.get("permissions", {}).get("is_admin"):
        target_branch_id = user.get("branch_id")

    new_expense = models.Expense(
        description=description, amount=amount, date=date,
        category=None, created_by=user['id'], branch_id=target_branch_id
    )
    db.add(new_expense)
    await db.commit()
    await db.refresh(new_expense)

    await log(
        db, user['id'], "create", "expense", new_expense.id,
        user.get('branch_id'), f"Dépense créée: {description} ({amount} TND)"
    )

    return RedirectResponse(request.url_for('expenses_page'), status_code=status.HTTP_302_FOUND)


@app.post("/expenses/{expense_id}/delete", name="expenses_delete_web")
async def expenses_delete_web(
    request: Request,
    expense_id: int,
    db: AsyncSession = Depends(get_db),
    # Require specific permission for consistency (Admin still overrides)
    user: dict = Depends(web_require_permission("can_manage_expenses"))
):
    res = await db.execute(select(models.Expense).where(models.Expense.id == expense_id))
    expense = res.scalar_one_or_none()
    
    if expense:
        # Hard Delete: Remove from database as requested
        await db.delete(expense)
        await db.commit()
        await log(
            db, user['id'], "delete", "expense", expense_id,
            user.get('branch_id'), f"Dépense supprimée: {expense.description}"
        )
        
    return RedirectResponse(request.url_for('expenses_page'), status_code=status.HTTP_302_FOUND)

# --- Congés (Leaves) ---
# ... (Leaves routes remain the same - not shown for brevity) ...
@app.get("/leaves", response_class=HTMLResponse, name="leaves_page")
async def leaves_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_leaves"))
):
    employees_query = select(Employee).where(Employee.active == True).order_by(Employee.first_name)
    # === FIX: Ajout du tri secondaire par created_at ===
    leaves_query = select(Leave).options(selectinload(Leave.employee), selectinload(Leave.creator)).order_by(Leave.start_date.desc(), Leave.created_at.desc()) # Charger l'employé
    # === FIN DU FIX ===

    permissions = user.get("permissions", {})
    
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch))
    all_branches = res_branches.scalars().all()

    if not permissions.get("is_admin"):
        branch_id = user.get("branch_id")
        employees_query = employees_query.where(Employee.branch_id == branch_id)
        leaves_query = leaves_query.join(Employee).where(Employee.branch_id == branch_id)
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             bid = int(branch_filter_id)
             employees_query = employees_query.where(Employee.branch_id == bid)
             leaves_query = leaves_query.join(Employee).where(Employee.branch_id == bid)

    res_employees = await db.execute(employees_query)
    res_leaves = await db.execute(leaves_query.limit(100))

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": res_employees.scalars().all(),
        "leaves": res_leaves.scalars().all(),
        "branches": all_branches, # Passed for Admin Selector
        "selected_branch_id": request.query_params.get("branch_id"), 
    }
    return templates.TemplateResponse("leaves.html", context)


@app.post("/leaves/create", name="leaves_create")
async def leaves_create(
    request: Request,
    employee_id: Annotated[int, Form()],
    start_date: Annotated[dt_date, Form()],
    end_date: Annotated[dt_date, Form()],
    ltype: Annotated[LeaveType, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_leaves"))
):
    if start_date > end_date:
        return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)

    res_emp = await db.execute(select(Employee).where(Employee.id == employee_id))
    employee = res_emp.scalar_one_or_none()
    if not employee:
        return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin") and user.get("branch_id") != employee.branch_id:
        return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)

    new_leave = Leave(
        employee_id=employee_id, start_date=start_date, end_date=end_date,
        ltype=ltype, approved=False, created_by=user['id']
    )
    db.add(new_leave)
    await db.commit()
    await db.refresh(new_leave)

    await log(
        db, user['id'], "create", "leave", new_leave.id,
        employee.branch_id, f"Congé pour Employé ID={employee_id}, Type={ltype.value}"
    )

    return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)


@app.post("/leaves/{leave_id}/approve", name="leaves_approve")
async def leaves_approve(
    request: Request,
    leave_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_leaves"))
):
    res_leave = await db.execute(
        select(Leave).options(selectinload(Leave.employee)).where(Leave.id == leave_id)
    )
    leave = res_leave.scalar_one_or_none()

    if not leave or leave.approved:
        return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin") and user.get("branch_id") != leave.employee.branch_id:
        return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)

    leave.approved = True
    await db.commit()

    await log(
        db, user['id'], "approve", "leave", leave.id,
        leave.employee.branch_id, f"Congé approuvé pour Employé ID={leave.employee_id}"
    )

    return RedirectResponse(request.url_for('leaves_page'), status_code=status.HTTP_302_FOUND)

@app.post("/leaves/{leave_id}/delete", name="leaves_delete")
async def leaves_delete(
    request: Request,
    leave_id: int,
    db: AsyncSession = Depends(get_db),
    # Only Admin can delete leaves for now, adjust permission if needed
    user: dict = Depends(web_require_permission("is_admin"))
):
    """Supprime une demande de congé."""

    # Fetch the leave record along with the employee
    # No need for branch check here as only admin can access
    leave_query = select(Leave).options(selectinload(Leave.employee)).where(Leave.id == leave_id)

    res_leave = await db.execute(leave_query)
    leave_to_delete = res_leave.scalar_one_or_none()

    if leave_to_delete:
        try:
            employee_name = f"{leave_to_delete.employee.first_name} {leave_to_delete.employee.last_name}" if leave_to_delete.employee else f"ID {leave_to_delete.employee_id}"
            leave_start = leave_to_delete.start_date
            leave_end = leave_to_delete.end_date
            emp_branch_id = leave_to_delete.employee.branch_id if leave_to_delete.employee else None

            await db.delete(leave_to_delete)
            await db.commit()

            # Log the deletion
            await log(
                db, user['id'], "delete", "leave", leave_id,
                emp_branch_id, f"Congé supprimé ({leave_start} à {leave_end}) pour {employee_name}"
            )
            await db.commit() # Commit the log entry

            print(f"✅ Congé ID={leave_id} supprimé avec succès.")

        except Exception as e:
            await db.rollback()
            print(f"ERREUR lors de la suppression du congé ID={leave_id}: {e}")
            traceback.print_exc()
            # Optionally add a flash message here

    else:
        # Leave record not found
        print(f"Tentative de suppression du congé ID={leave_id} échouée (non trouvé).")

    # Redirect back to the leaves list page
    return RedirectResponse(request.url_for("leaves_page"), status_code=status.HTTP_302_FOUND)

# --- Rapport Employé ---
# ... (Employee Report route remains the same - not shown for brevity) ...

@app.get("/employee-report", response_class=HTMLResponse, name="employee_report_index")
async def employee_report_index(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_view_reports")),
    employee_id: int | None = None
):
    employees_query = select(Employee).where(Employee.active == True).order_by(Employee.first_name)
    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        employees_query = employees_query.where(Employee.branch_id == user.get("branch_id"))

    res_employees = await db.execute(employees_query)
    employees_list = res_employees.scalars().all()

    selected_employee = None
    pay_history = []
    deposits = []
    absences = []
    leaves = []
    loans = []

    # --- Create default summary variables ---
    summary_advances = 0
    summary_absences = 0
    summary_is_paid = False
    summary_paid_amount = 0
    summary_active_leaves = []
    summary_has_loan = False
    summary_primes = 0 # <-- NOUVEAU: Ajout de la variable pour les primes
    period_label = "Résumé"
    # --- End defaults ---

    if employee_id:
        # ... (permission check logic remains the same) ...
        employee_visible = False
        if permissions.get("is_admin"):
             employee_visible = True
        else:
             for emp in employees_list: # Vérifier dans la liste filtrée
                 if emp.id == employee_id:
                     employee_visible = True
                     break

        # --- Initialize default values for summary variables ---
        summary_advances = 0
        summary_absences = 0
        summary_is_paid = False
        summary_paid_amount = 0
        summary_active_leaves = []
        summary_has_loan = False
        summary_primes = 0
        period_label = "Résumé"
        # -------------------------------------------------------

        if employee_visible:
            res_selected = await db.execute(select(Employee).where(Employee.id == employee_id))
            selected_employee = res_selected.scalar_one_or_none()

            if selected_employee:
                # === FIX: Ajout de .order_by(..., Model.created_at.desc()) pour trier par "Enregistré le" ===
                res_pay = await db.execute(select(Pay).where(Pay.employee_id == employee_id).order_by(Pay.date.desc(), Pay.created_at.desc()))
                pay_history = res_pay.scalars().all()
                res_dep = await db.execute(select(Deposit).where(Deposit.employee_id == employee_id).order_by(Deposit.date.desc(), Deposit.created_at.desc()))
                deposits = res_dep.scalars().all()
                res_abs = await db.execute(select(Attendance).where(Attendance.employee_id == employee_id).order_by(Attendance.date.desc(), Attendance.created_at.desc()))
                absences = res_abs.scalars().all()
                res_lea = await db.execute(select(Leave).where(Leave.employee_id == employee_id).order_by(Leave.start_date.desc(), Leave.created_at.desc()))
                leaves = res_lea.scalars().all()
                res_loans = await db.execute(select(Loan).where(Loan.employee_id == employee_id).order_by(Loan.start_date.desc(), Loan.created_at.desc()))
                loans = res_loans.scalars().all()
                # === FIN DU FIX ===

                # ===== START: New Summary Logic (Weekly/Monthly) =====
                today = datetime.now(TUNISIA_TZ)
                
                # Determine Period based on Salary Frequency
                # Default to monthly if not set
                freq = selected_employee.salary_frequency # Enum
                
                period_label = "Résumé du Mois Actuel"
                period_start = today.date().replace(day=1)
                # End of month approximation (for query usually just >= start is enough if we filter by current month, 
                # but strict range is better).
                # Simple way to get end of month:
                next_month = today.date().replace(day=28) + timedelta(days=4)
                period_end = next_month - timedelta(days=next_month.day)

                if freq == SalaryFrequency.weekly:
                    period_label = "Résumé de la Semaine Actuelle"
                    # Monday = 0, Sunday = 6
                    # We want Mon to Sun ? Or user specific ? Usually Mon-Sun in Tunisia business or Sat-Fri?
                    # Let's assume Mon-Sun for ISO standard, or ask user?
                    # "Standard" is usually Mon-Sun.
                    # today.weekday() returns 0 for Mon, 6 for Sun.
                    start_of_week = today.date() - timedelta(days=today.weekday())
                    end_of_week = start_of_week + timedelta(days=6)
                    period_start = start_of_week
                    period_end = end_of_week
                
                # 1. Advances (Deposits) in Period
                res_period_advances = await db.execute(
                    select(models.Deposit).where(
                        models.Deposit.employee_id == employee_id,
                        models.Deposit.date >= period_start,
                        models.Deposit.date <= period_end
                    )
                )
                period_advances_list = res_period_advances.scalars().all()
                summary_advances = sum(d.amount for d in period_advances_list)

                # 2. Absences in Period
                res_period_absences = await db.execute(
                    select(models.Attendance).where(
                        models.Attendance.employee_id == employee_id,
                        models.Attendance.atype == AttendanceType.absent,
                        models.Attendance.date >= period_start,
                        models.Attendance.date <= period_end
                    )
                )
                summary_absences = len(res_period_absences.scalars().all())

                # 3. Salary Payment in Period
                # We check if a "Main" salary payment (mensuel OR hebdomadaire) exists in this period
                # Adjust PayType check based on frequency? 
                # Actually, simply check if ANY "Salary" type payment occurred.
                target_pay_type = PayType.mensuel if freq == 'monthly' else PayType.hebdomadaire
                
                res_period_salary = await db.execute(
                    select(models.Pay).where(
                        models.Pay.employee_id == employee_id,
                        models.Pay.pay_type == target_pay_type, 
                        models.Pay.date >= period_start,
                        models.Pay.date <= period_end
                    )
                )
                period_salary_payments_list = res_period_salary.scalars().all()
                summary_is_paid = len(period_salary_payments_list) > 0
                summary_paid_amount = sum(p.amount for p in period_salary_payments_list) if summary_is_paid else 0

                # 4. Check for active or upcoming leaves (uses 'leaves' list)
                summary_active_leaves = [l for l in leaves if l.end_date >= today.date()]

                # 5. Check for active loans (uses 'loans' list)
                summary_active_loans_list = [
                    loan for loan in loans 
                    if loan.status.value in ('active', 'approuvé', 'approved')
                ]
                summary_has_loan = len(summary_active_loans_list) > 0
                
                # 6. Primes int Period
                res_period_primes = await db.execute(
                    select(models.Pay).where(
                        models.Pay.employee_id == employee_id,
                        models.Pay.pay_type == PayType.prime_rendement,
                        models.Pay.date >= period_start,
                        models.Pay.date <= period_end
                    )
                )
                period_primes_list = res_period_primes.scalars().all()
                summary_primes = sum(p.amount for p in period_primes_list)
                
                # ===== END: New Summary Logic =====

        else:
            employee_id = None # Ne pas montrer les données si pas autorisé
            period_label = "Résumé" # Fallback

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": employees_list, "selected_employee": selected_employee,
        "pay_history": pay_history, "deposits": deposits,
        "absences": absences, "leaves": leaves, "loans": loans,
        "current_employee_id": employee_id,
        
        # --- Add these new variables for the Summary Box ---
        "summary_advances": summary_advances,
        "summary_absences": summary_absences,
        "summary_is_paid": summary_is_paid,
        "summary_paid_amount": summary_paid_amount,
        "summary_active_leaves": summary_active_leaves,
        "summary_has_loan": summary_has_loan,
        "summary_primes": summary_primes,
        "period_label": period_label # <-- DYNAMIC LABEL
    }
    return templates.TemplateResponse("employee_report.html", context)


# --- Payer Employé ---
# ... (Pay Employee routes remain the same - not shown for brevity) ...
@app.get("/pay-employee", response_class=HTMLResponse, name="pay_employee_page")
async def pay_employee_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_pay"))
):
    employees_query = select(Employee).where(Employee.active == True).order_by(Employee.first_name)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        employees_query = employees_query.where(Employee.branch_id == user.get("branch_id"))

    res_employees = await db.execute(employees_query)

    # --- NOUVEAU: Charger les paiements récents (pour le tableau dans pay_employee.html) ---
    recent_payments_query = (
        select(Pay)
        .options(selectinload(Pay.employee), selectinload(Pay.creator))
        .order_by(Pay.date.desc(), Pay.created_at.desc())
    )
    
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch))
    all_branches = res_branches.scalars().all()

    if not permissions.get("is_admin"):
        employees_query = employees_query.where(Employee.branch_id == user.get("branch_id"))
        recent_payments_query = recent_payments_query.join(Employee).where(Employee.branch_id == user.get("branch_id"))
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             bid = int(branch_filter_id)
             employees_query = employees_query.where(Employee.branch_id == bid)
             recent_payments_query = recent_payments_query.join(Employee).where(Employee.branch_id == bid)
        
    res_recent_payments = await db.execute(recent_payments_query.limit(10))
    recent_payments = res_recent_payments.scalars().all()
    # --- FIN NOUVEAU ---


    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": res_employees.scalars().all(),
        "branches": all_branches, # Passed for Admin Selector
        "selected_branch_id": request.query_params.get("branch_id"), # For UI state
        "today_date": get_tunisia_today().isoformat(),
        "recent_payments": recent_payments # <-- NOUVEAU: Ajout au contexte
    }
    return templates.TemplateResponse("pay_employee.html", context)


@app.post("/pay-employee", name="pay_employee_action")
async def pay_employee_action(
    request: Request,
    employee_id: Annotated[int, Form()],
    amount: Annotated[Decimal, Form()],
    date: Annotated[dt_date, Form()],
    pay_type: Annotated[PayType, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_pay")),
    note: Annotated[str, Form()] = None
):
    res_emp = await db.execute(select(Employee).where(Employee.id == employee_id))
    employee = res_emp.scalar_one_or_none()

    if not employee or amount <= 0:
        return RedirectResponse(request.url_for('pay_employee_page'), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin") and user.get("branch_id") != employee.branch_id:
        return RedirectResponse(request.url_for('pay_employee_page'), status_code=status.HTTP_302_FOUND)

    new_pay = Pay(
        employee_id=employee_id, amount=amount, date=date,
        pay_type=pay_type, note=note or None, created_by=user['id']
    )
    db.add(new_pay)
    await db.commit()
    await db.refresh(new_pay)

    await log(
        db, user['id'], "create", "pay", new_pay.id,
        employee.branch_id, f"Paiement pour Employé ID={employee_id}, Montant={amount}, Type={pay_type.value}"
    )

    # --- MODIFIÉ : Rediriger vers la page de paiement si c'est une prime, sinon vers le rapport ---
    if pay_type == PayType.prime_rendement:
         return RedirectResponse(
            request.url_for('pay_employee_page'), # Rester sur la page de paiement
            status_code=status.HTTP_302_FOUND
         )
    
    return RedirectResponse(
        str(request.url_for('employee_report_index')) + f"?employee_id={employee_id}",
        status_code=status.HTTP_302_FOUND
    )

@app.post("/pay-history/{pay_id}/delete", name="pay_history_delete")
async def pay_history_delete(
    request: Request,
    pay_id: int,
    db: AsyncSession = Depends(get_db),
    # Only Admin should delete pay records directly? Adjust permission if needed.
    user: dict = Depends(web_require_permission("is_admin")),
    # Get employee_id from form to redirect back correctly
    employee_id: int = Form(...)
):
    """Supprime un enregistrement de l'historique de paie."""

    # Fetch the pay record along with the employee to check permissions if needed
    # Since only admin can delete, we might not need detailed permission check here,
    # but fetching employee helps with logging.
    pay_query = select(Pay).options(selectinload(Pay.employee)).where(Pay.id == pay_id)

    res_pay = await db.execute(pay_query)
    pay_to_delete = res_pay.scalar_one_or_none()

    redirect_url = request.url_for("employee_report_index")
    if employee_id:
        redirect_url = str(redirect_url) + f"?employee_id={employee_id}"


    if pay_to_delete:
        try:
            # Get info for logging before deleting
            employee_name = f"{pay_to_delete.employee.first_name} {pay_to_delete.employee.last_name}" if pay_to_delete.employee else f"ID {pay_to_delete.employee_id}"
            pay_date = pay_to_delete.date
            pay_amount = pay_to_delete.amount
            emp_branch_id = pay_to_delete.employee.branch_id if pay_to_delete.employee else None

            await db.delete(pay_to_delete)
            await db.commit()

            # Log the deletion
            await log(
                db, user['id'], "delete", "pay", pay_id, # Use 'pay' as entity type
                emp_branch_id, f"Paiement supprimé ({pay_amount} TND) pour {employee_name} du {pay_date}"
            )
            await db.commit() # Commit the log entry

            print(f"✅ Paiement ID={pay_id} supprimé avec succès.")

        except Exception as e:
            await db.rollback()
            print(f"ERREUR lors de la suppression du paiement ID={pay_id}: {e}")
            traceback.print_exc()
            # Optionally add a flash message here

    else:
        # Pay record not found
        print(f"Tentative de suppression du paiement ID={pay_id} échouée (non trouvé).")

    # Redirect back to the specific employee's report page
    return RedirectResponse(redirect_url, status_code=status.HTTP_302_FOUND)

#
# --- MODIFICATION: ROUTES POUR LES PRIMES ---
#
@app.get("/primes", response_class=HTMLResponse, name="primes_page")
async def primes_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_pay")), # Utilise la même permission que les paiements
    start_date: str | None = None,
    end_date: str | None = None,
    branch_id: str | None = None # Peut être "all" ou un ID
):
    """Affiche la page du classement des primes."""
    
    today = get_tunisia_today()
    
    # Gérer les dates par défaut (30 derniers jours)
    try:
        parsed_end_date = dt_date.fromisoformat(end_date) if end_date else today
    except ValueError:
        parsed_end_date = today
        
    try:
        parsed_start_date = dt_date.fromisoformat(start_date) if start_date else (today - timedelta(days=30))
    except ValueError:
        parsed_start_date = today - timedelta(days=30)

    selected_start_date_iso = parsed_start_date.isoformat()
    selected_end_date_iso = parsed_end_date.isoformat()
    selected_branch_id = branch_id # Garder "all" ou l'ID
    
    permissions = user.get("permissions", {})

    # 1. Get available branches for the filter
    branches_query = select(Branch).order_by(Branch.name)
    if not permissions.get("is_admin"):
        branches_query = branches_query.where(Branch.id == user.get("branch_id"))
    
    res_branches = await db.execute(branches_query)
    branches = res_branches.scalars().all()

    # 2. Get employees visible by the user, filtered by selected branch
    employees_query = select(Employee).where(Employee.active == True)
    
    if not permissions.get("is_admin"):
        # Un manager ne voit que sa branche
        employees_query = employees_query.where(Employee.branch_id == user.get("branch_id"))
        # Si un manager est sur sa seule branche, on force le selected_branch_id
        if branches:
            selected_branch_id = str(branches[0].id)
    else:
        # L'admin peut filtrer
        if selected_branch_id and selected_branch_id != "all":
            try:
                employees_query = employees_query.where(Employee.branch_id == int(selected_branch_id))
            except ValueError:
                pass # Ignorer si branch_id n'est pas un entier valide

    res_employees = await db.execute(employees_query)
    employees = res_employees.scalars().all()
    employee_ids = [e.id for e in employees]

    if not employee_ids:
        # No employees to rank
        context = {
            "request": request, "user": user, "app_name": APP_NAME,
            "sorted_employees": [],
            "branches": branches,
            "selected_start_date": selected_start_date_iso,
            "selected_end_date": selected_end_date_iso,
            "selected_branch_id": selected_branch_id
        }
        return templates.TemplateResponse("primes.html", context)

    # 3. Get aggregated stats for these employees for the selected date range
    
    # Subquery for absences
    sub_absences = (
        select(
            Attendance.employee_id,
            func.count(Attendance.id).label("absence_count")
        )
        .where(
            Attendance.employee_id.in_(employee_ids),
            Attendance.atype == AttendanceType.absent,
            Attendance.date.between(parsed_start_date, parsed_end_date)
        )
        .group_by(Attendance.employee_id)
    ).subquery()

    # Subquery for advances (Deposits)
    sub_avances = (
        select(
            Deposit.employee_id,
            func.sum(Deposit.amount).label("avance_total")
        )
        .where(
            Deposit.employee_id.in_(employee_ids),
            Deposit.date.between(parsed_start_date, parsed_end_date)
        )
        .group_by(Deposit.employee_id)
    ).subquery()

    # Subquery for sales stats
    sales_query = select(
        SalesSummary.employee_id,
        func.sum(SalesSummary.quantity_sold).label("total_qty"),
        func.sum(SalesSummary.total_revenue).label("total_rev")
    ).where(
        SalesSummary.employee_id.in_(employee_ids),
        SalesSummary.date.between(parsed_start_date, parsed_end_date)
    )

    # --- FILTER BY STORE NAME IF BRANCH SELECTED ---
    # The user wants "separate" sales per store.
    # If a branch is selected, filter SalesSummary by store_name which typically matches Branch.name
    filter_store_name = None
    if selected_branch_id and str(selected_branch_id).lower() != "all":
        try:
            bid = int(selected_branch_id)
            # Find branch name from the loaded branches list (optimized)
            found_branch = next((b for b in branches if b.id == bid), None)
            if found_branch:
                filter_store_name = found_branch.name
        except ValueError:
            pass

    if filter_store_name:
        sales_query = sales_query.where(SalesSummary.store_name == filter_store_name)
    
    sub_sales = sales_query.group_by(SalesSummary.employee_id).subquery()

    # 4. Join employees with their stats
    stmt = (
        select(
            Employee,
            func.coalesce(sub_absences.c.absence_count, 0).label("absences"),
            func.coalesce(sub_avances.c.avance_total, Decimal(0)).label("avances"), # Assurer le type Decimal
            func.coalesce(sub_sales.c.total_qty, 0).label("sales_qty"),
            func.coalesce(sub_sales.c.total_rev, Decimal(0)).label("sales_rev")
        )
        .outerjoin(sub_absences, Employee.id == sub_absences.c.employee_id)
        .outerjoin(sub_avances, Employee.id == sub_avances.c.employee_id)
        .outerjoin(sub_sales, Employee.id == sub_sales.c.employee_id)
        .where(Employee.id.in_(employee_ids)) # Appliquer le filtre des employés visibles
    )

    results = await db.execute(stmt)
    
    # 5. Calculate score for each
    employee_stats_list = []
    for emp, absences, avances, sales_qty, sales_rev in results:
        # Score: 1000 - (100 * absences) - (0.5 * avances en TND) + (Sales bonus?)
        # For now, sales data is just for visibility, not affecting score yet (unless user asked).
        # User said "countity sells how many number and total what he sell" in primes page.
        # User did NOT ask for score formula update yet, just display.
        
        if not sales_qty: sales_qty = 0
        if not sales_rev: sales_rev = 0.0

        # Score Formula:
        # Base: 1000
        # - Absences: 50 points each
        # + Sales (Qty): 2 points per item (invoice)
        # + Sales (Rev): 1 point per 100 TND (0.01 * Rev)
        
        score = 1000.0
        score -= (float(absences) * 50.0)
        score += (float(sales_qty) * 2.0)
        score += (float(sales_rev) * 0.01)
        
        if score < 0:
            score = 0
        
        employee_stats_list.append({
            "id": emp.id,
            "name": f"{emp.first_name} {emp.last_name}",
            "absences": int(absences),
            "avances": float(avances),
            "sales_qty": int(sales_qty),
            "sales_rev": float(sales_rev),
            "score": round(score)
        })
        
    # 6. Sort by score descending
    sorted_employees = sorted(employee_stats_list, key=lambda x: x["score"], reverse=True)

    # 7. Render template
    context = {
        "request": request,
        "user": user,
        "app_name": APP_NAME,
        "sorted_employees": sorted_employees,
        "branches": branches, # Passer les magasins au template
        "selected_start_date": selected_start_date_iso,
        "selected_end_date": selected_end_date_iso,
        "selected_branch_id": selected_branch_id
    }
    return templates.TemplateResponse("primes.html", context)


# --- NOUVEAU : Route pour attribuer les primes (remplace l'ancienne) ---
@app.post("/primes/attribuer", name="primes_attribuer")
async def primes_attribuer(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_pay"))
):
    """Enregistre les paiements de primes pour plusieurs employés."""
    
    today = get_tunisia_today()
    form_data = await request.form()
    
    # Récupérer les filtres pour la redirection
    start_date = form_data.get("start_date", "")
    end_date = form_data.get("end_date", "")
    branch_id = form_data.get("branch_id", "all")

    primes_to_pay = []
    emp_ids_to_check = []
    
    # Analyser le formulaire pour trouver les primes
    for key, value in form_data.items():
        if key.startswith("prime_") and value:
            try:
                emp_id = int(key.split("_")[1])
                amount = Decimal(value)
                
                if amount > 0:
                    primes_to_pay.append((emp_id, amount))
                    emp_ids_to_check.append(emp_id)
                    
            except (IndexError, ValueError, TypeError):
                print(f"AVERTISSEMENT: Donnée de prime invalide reçue: key={key}, value={value}")
                continue

    if not primes_to_pay:
        # Aucune prime valide entrée, rediriger simplement
        redirect_url = f"{request.url_for('primes_page')}?start_date={start_date}&end_date={end_date}&branch_id={branch_id}"
        return RedirectResponse(redirect_url, status_code=status.HTTP_302_FOUND)

    # Vérifier les permissions sur les employés
    emp_query = select(Employee).where(Employee.id.in_(emp_ids_to_check))
    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        emp_query = emp_query.where(Employee.branch_id == user.get("branch_id"))
    
    res_emps = await db.execute(emp_query)
    allowed_employees = {e.id: e for e in res_emps.scalars()}

    # Enregistrer les paiements
    for emp_id, amount in primes_to_pay:
        if emp_id not in allowed_employees:
            print(f"AVERTISSEMENT: L'utilisateur {user['id']} a tenté de payer une prime à l'employé {emp_id} hors de sa branche.")
            continue # Ignorer ce paiement

        employee = allowed_employees[emp_id]
        note_text = f"Prime (Période: {start_date} à {end_date})"
        
        new_pay = Pay(
            employee_id=emp_id,
            amount=amount,
            date=today, # Payé aujourd'hui
            pay_type=PayType.prime_rendement, # Type correct
            note=note_text,
            created_by=user['id']
        )
        db.add(new_pay)
        
        # Log l'action
        await log(
            db, user['id'], "create", "pay", None, # L'ID sera défini après le flush
            employee.branch_id,
            f"Paiement (Prime) pour {employee.first_name} {employee.last_name}, Montant={amount}, Motif: {note_text}"
        )
    
    try:
        await db.commit() # Commit tous les paiements et logs
    except Exception as e:
        await db.rollback()
        print(f"Erreur lors de l'enregistrement des primes: {e}")
        # Gérer l'erreur (ex: message flash)

    # Rediriger vers la page des primes avec les mêmes filtres
    redirect_url = f"{request.url_for('primes_page')}?start_date={start_date}&end_date={end_date}&branch_id={branch_id}"
    return RedirectResponse(redirect_url, status_code=status.HTTP_302_FOUND)
# --- FIN DES ROUTES PRIMES ---


# --- Gestion des Rôles ---
# ... (Roles routes remain the same - not shown for brevity) ...
@app.get("/loans", response_class=HTMLResponse, name="loans_page")
async def loans_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_loans"))
):
    # Query for Employees
    q_emp = select(Employee).where(Employee.active == True).order_by(Employee.first_name)
    
    # Query for Loans
    q_loans = select(Loan).options(selectinload(Loan.employee), selectinload(Loan.creator)).order_by(Loan.created_at.desc())

    permissions = user.get("permissions", {})
    
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch))
    all_branches = res_branches.scalars().all()

    if not permissions.get("is_admin"):
        branch_id = user.get("branch_id")
        q_emp = q_emp.where(Employee.branch_id == branch_id)
        q_loans = q_loans.join(Employee).where(Employee.branch_id == branch_id)
    else:
        # Admin Filter
        branch_filter_id = request.query_params.get("branch_id")
        if branch_filter_id and branch_filter_id.isdigit():
             bid = int(branch_filter_id)
             q_emp = q_emp.where(Employee.branch_id == bid)
             q_loans = q_loans.join(Employee).where(Employee.branch_id == bid)

    res_emp = await db.execute(q_emp)
    res_loans = await db.execute(q_loans)

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "employees": res_emp.scalars().all(),
        "loans": res_loans.scalars().all(),
        "branches": all_branches, # Passed for Admin Selector
        "selected_branch_id": request.query_params.get("branch_id"), 
        "today_date": get_tunisia_today().isoformat()
    }
    return templates.TemplateResponse("loans.html", context)


@app.get("/roles", response_class=HTMLResponse, name="roles_page")
async def roles_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_roles"))
):
    res_roles = await db.execute(
        select(Role).options(selectinload(Role.users)).order_by(Role.name)
    )

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "roles": res_roles.scalars().unique().all()
    }
    return templates.TemplateResponse("roles.html", context)


@app.post("/roles/create", name="roles_create")
async def roles_create(
    request: Request,
    name: Annotated[str, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_roles"))
):
    res_exist = await db.execute(select(Role).where(Role.name == name))
    if res_exist.scalar_one_or_none():
        return RedirectResponse(request.url_for('roles_page'), status_code=status.HTTP_302_FOUND)

    new_role = Role(name=name)
    db.add(new_role)

    await db.commit()
    await db.refresh(new_role)

    await log(
        db, user['id'], "create", "role", new_role.id,
        None, f"Rôle créé: {new_role.name}"
    )

    return RedirectResponse(request.url_for('roles_page'), status_code=status.HTTP_302_FOUND)


@app.post("/roles/{role_id}/update", name="roles_update")
async def roles_update(
    request: Request,
    role_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_roles"))
):
    res_role = await db.execute(select(Role).where(Role.id == role_id))
    role_to_update = res_role.scalar_one_or_none()

    if not role_to_update or role_to_update.is_admin:
        return RedirectResponse(request.url_for('roles_page'), status_code=status.HTTP_302_FOUND)

    form_data = await request.form()

    role_to_update.can_manage_users = "can_manage_users" in form_data
    role_to_update.can_manage_roles = "can_manage_roles" in form_data
    role_to_update.can_manage_branches = "can_manage_branches" in form_data
    role_to_update.can_view_settings = "can_view_settings" in form_data
    role_to_update.can_clear_logs = "can_clear_logs" in form_data
    role_to_update.can_manage_employees = "can_manage_employees" in form_data
    role_to_update.can_view_reports = "can_view_reports" in form_data
    role_to_update.can_manage_pay = "can_manage_pay" in form_data
    role_to_update.can_manage_absences = "can_manage_absences" in form_data
    role_to_update.can_manage_leaves = "can_manage_leaves" in form_data
    role_to_update.can_manage_deposits = "can_manage_deposits" in form_data
    role_to_update.can_manage_loans = "can_manage_loans" in form_data
    role_to_update.can_manage_expenses = "can_manage_expenses" in form_data

    await db.commit()

    await log(
        db, user['id'], "update", "role", role_to_update.id,
        None, f"Permissions mises à jour pour le rôle: {role_to_update.name}"
    )

    return RedirectResponse(request.url_for('roles_page'), status_code=status.HTTP_302_FOUND)


@app.post("/roles/{role_id}/delete", name="roles_delete")
async def roles_delete(
    request: Request,
    role_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_roles"))
):
    res_role = await db.execute(
        select(Role).options(selectinload(Role.users)).where(Role.id == role_id)
    )
    role_to_delete = res_role.scalar_one_or_none()

    if not role_to_delete or role_to_delete.is_admin or len(role_to_delete.users) > 0:
        return RedirectResponse(request.url_for('roles_page'), status_code=status.HTTP_302_FOUND)

    role_name = role_to_delete.name
    await db.delete(role_to_delete)
    await db.commit()

    await log(
        db, user['id'], "delete", "role", role_id,
        None, f"Rôle supprimé: {role_name}"
    )

    return RedirectResponse(request.url_for('roles_page'), status_code=status.HTTP_302_FOUND)


# --- Gestion des Utilisateurs ---
# ... (Users routes remain the same - not shown for brevity) ...
@app.get("/users", response_class=HTMLResponse, name="users_page")
async def users_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_users"))
):
    res_users = await db.execute(
        select(User).options(selectinload(User.branch), selectinload(User.permissions)).order_by(User.full_name)
    )
    res_branches = await db.execute(select(Branch).order_by(Branch.name))
    res_roles = await db.execute(select(Role).order_by(Role.name))

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "users": res_users.scalars().unique().all(),
        "branches": res_branches.scalars().all(),
        "roles": res_roles.scalars().all(),
    }
    return templates.TemplateResponse("users.html", context)


@app.post("/users/create", name="users_create")
async def users_create(
    request: Request,
    full_name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    password: Annotated[str, Form()],
    role_id: Annotated[int, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_users")),
    branch_id: Annotated[int, Form()] = None,
):
    res_exist = await db.execute(select(User).where(User.email == email))
    if res_exist.scalar_one_or_none():
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    res_role = await db.execute(select(Role).where(Role.id == role_id))
    role = res_role.scalar_one_or_none()
    if not role:
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    final_branch_id = branch_id
    if role.is_admin:
        final_branch_id = None

    new_user = User(
        full_name=full_name, email=email,
        hashed_password=hash_password(password),
        role_id=role_id, branch_id=final_branch_id, is_active=True
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    await log(
        db, user['id'], "create", "user", new_user.id,
        new_user.branch_id, f"Utilisateur créé: {new_user.email} (Rôle: {role.name})"
    )

    return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)


@app.post("/users/{user_id}/update", name="users_update")
async def users_update(
    request: Request,
    user_id: int,
    full_name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    role_id: Annotated[int, Form()],
    is_active: Annotated[bool, Form()] = False,
    branch_id: Annotated[int, Form()] = None,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_users")),
):
    res_user = await db.execute(select(User).options(selectinload(User.permissions)).where(User.id == user_id))
    user_to_update = res_user.scalar_one_or_none()
    if not user_to_update:
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    if user_to_update.email != email:
        res_exist = await db.execute(select(User).where(User.email == email, User.id != user_id))
        if res_exist.scalar_one_or_none():
            return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    res_role = await db.execute(select(Role).where(Role.id == role_id))
    role = res_role.scalar_one_or_none()
    if not role:
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    final_branch_id = branch_id
    if role.is_admin:
        final_branch_id = None

    if user_to_update.permissions.is_admin:
        if user_to_update.id == user['id'] and (not is_active or not role.is_admin):
             return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    user_to_update.full_name = full_name
    user_to_update.email = email
    user_to_update.role_id = role_id
    user_to_update.branch_id = final_branch_id
    user_to_update.is_active = is_active

    await db.commit()

    await log(
        db, user['id'], "update", "user", user_to_update.id,
        user_to_update.branch_id, f"Utilisateur mis à jour: {user_to_update.email}"
    )

    return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)


@app.post("/users/{user_id}/password", name="users_password")
async def users_password(
    request: Request,
    user_id: int,
    password: Annotated[str, Form()],
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_users")),
):
    res_user = await db.execute(select(User).options(selectinload(User.permissions)).where(User.id == user_id))
    user_to_update = res_user.scalar_one_or_none()

    if not user_to_update or len(password) < 1:
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    user_to_update.hashed_password = hash_password(password)
    await db.commit()

    await log(
        db, user['id'], "update_password", "user", user_to_update.id,
        user_to_update.branch_id, f"Mot de passe réinitialisé pour: {user_to_update.email}"
    )

    return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)


@app.post("/users/{user_id}/delete", name="users_delete")
async def users_delete(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_users")),
):
    if user['id'] == user_id:
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    res_user = await db.execute(select(User).where(User.id == user_id))
    user_to_delete = res_user.scalar_one_or_none()

    if not user_to_delete:
        return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)

    user_email = user_to_delete.email
    user_branch_id = user_to_delete.branch_id

    await db.delete(user_to_delete)
    await db.commit()

    await log(
        db, user['id'], "delete", "user", user_id,
        user_branch_id, f"Utilisateur supprimé: {user_email}"
    )

    return RedirectResponse(request.url_for('users_page'), status_code=status.HTTP_302_FOUND)


# --- Page Paramètres ---
# ... (Settings route remains the same) ...
@app.get("/settings", response_class=HTMLResponse, name="settings_page")
async def settings_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_view_settings"))
):
    permissions = user.get("permissions", {})
    filtered_logs = await latest(
        db,
        user_is_admin=permissions.get("is_admin", False),
        branch_id=user.get("branch_id"),
        # --- FIX: Inclure 'loan' dans les types d'entités pour le log ---
        entity_types=["leave", "attendance", "deposit", "pay", "loan"]
    )

    context = {
        "request": request, "user": user, "app_name": APP_NAME,
        "logs": filtered_logs
    }
    return templates.TemplateResponse("settings.html", context)


# --- 6. Route de Nettoyage (Corrigée) ---
# ... (Clear Logs route remains the same) ...
@app.post("/settings/clear-logs", name="clear_logs")
async def clear_transaction_logs(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_clear_logs"))
):
    print(f"ACTION ADMIN (user {user['id']}): Nettoyage des journaux...")

    try:
        # Supprimer dans l'ordre inverse des dépendances pour éviter les erreurs de contrainte
        await db.execute(delete(AuditLog))
        await db.execute(delete(LoanRepayment))
        await db.execute(delete(LoanSchedule))
        await db.execute(delete(Loan))
        await db.execute(delete(Pay))
        await db.execute(delete(Deposit))
        await db.execute(delete(models.Expense)) # Added Expenses
        await db.execute(delete(Leave))
        await db.execute(delete(Attendance))

        await db.commit()
        print("✅ Nettoyage des journaux terminé avec succès.")

        await log(
            db, user['id'], "delete", "all_logs", None,
            None, "Toutes les données transactionnelles ont été supprimées."
        )
        await db.commit()

    except Exception as e:
        await db.rollback()
        print(f"ERREUR lors du nettoyage des journaux: {e}")

    return RedirectResponse(request.url_for('settings_page'), status_code=status.HTTP_302_FOUND)

#
# --- NOUVEAU : FONCTIONNALITÉS DE BACKUP / RESTORE ---
#
@app.get("/settings/export", name="export_data")
async def export_data(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("is_admin")) # Admin seulement
):
    """Exporte toutes les données de la base de données en JSON."""

    data_to_export = {}

    try:
        # Exporter chaque table
        data_to_export["branches"] = (await db.execute(select(Branch))).scalars().all()

        # --- FIX: Inclure hashed_password dans l'export ---
        # L'encodeur JSON personnalisé va maintenant inclure toutes les colonnes par défaut
        data_to_export["users"] = (await db.execute(select(User))).scalars().all()
        # --- FIN FIX ---

        data_to_export["employees"] = (await db.execute(select(Employee))).scalars().all()
        data_to_export["attendance"] = (await db.execute(select(Attendance))).scalars().all()
        data_to_export["leaves"] = (await db.execute(select(Leave))).scalars().all()
        data_to_export["deposits"] = (await db.execute(select(Deposit))).scalars().all()
        data_to_export["pay_history"] = (await db.execute(select(Pay))).scalars().all()
        data_to_export["loans"] = (await db.execute(select(Loan))).scalars().all()
        data_to_export["loan_schedules"] = (await db.execute(select(LoanSchedule))).scalars().all()
        data_to_export["loan_repayments"] = (await db.execute(select(LoanRepayment))).scalars().all()
        data_to_export["roles"] = (await db.execute(select(Role))).scalars().all()
        data_to_export["expenses"] = (await db.execute(select(models.Expense))).scalars().all() # Added Expenses
        data_to_export["audit_logs"] = (await db.execute(select(AuditLog).order_by(AuditLog.created_at))).scalars().all()


    except Exception as e:
        print(f"Erreur pendant l'export: {e}")
        # Log the full traceback for debugging
        import traceback
        traceback.print_exc()
        return RedirectResponse(request.url_for('settings_page'), status_code=status.HTTP_302_FOUND)

    # Créer un fichier JSON en mémoire
    try:
        json_data = json.dumps(data_to_export, cls=CustomJSONEncoder, indent=2, ensure_ascii=False) # Added ensure_ascii=False
    except Exception as e:
        print(f"Erreur pendant l'encodage JSON: {e}")
        import traceback
        traceback.print_exc()
        return RedirectResponse(request.url_for('settings_page'), status_code=status.HTTP_302_FOUND)

    file_stream = io.BytesIO(json_data.encode("utf-8"))

    filename = f"backup_bijouterie_zaher_{get_tunisia_today().isoformat()}.json"

    return StreamingResponse(
        file_stream,
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.post("/settings/import", name="import_data")
async def import_data(
    request: Request,
    backup_file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("is_admin")) # Admin seulement
):
    """Importe et restaure les données depuis un fichier JSON."""

    if not backup_file.filename.endswith(".json"):
        return RedirectResponse(request.url_for('settings_page'), status_code=status.HTTP_302_FOUND)

    try:
        contents = await backup_file.read()
        data = json.loads(contents.decode("utf-8"))

        # --- DANGER : SUPPRESSION DES DONNÉES ---
        await db.execute(delete(AuditLog))
        await db.execute(delete(LoanRepayment))
        await db.execute(delete(LoanSchedule))
        await db.execute(delete(Loan))
        await db.execute(delete(Pay))
        await db.execute(delete(Deposit))
        await db.execute(delete(Leave))
        await db.execute(delete(Attendance))
        await db.execute(delete(models.Expense)) # Added
        await db.execute(delete(Employee))
        await db.execute(delete(User))
        await db.execute(delete(Role)) # Added Role deletion (after User)
        await db.execute(delete(Branch))

        # --- RÉINSERTION DES DONNÉES ---

        # Helper function to safely convert string to Enum
        def get_enum_member(enum_cls, value, default=None):
            if value is None:
                return default
            try:
                return enum_cls(value)
            except ValueError:
                print(f"AVERTISSEMENT: Valeur d'énumération invalide '{value}' pour {enum_cls.__name__}. Utilisation de la valeur par défaut {default}.")
                return default

        if "branches" in data:
            for item in data["branches"]:
                item = _parse_dates(item, datetime_fields=['created_at'])
                db.add(Branch(**item))
        await db.flush()

        if "roles" in data:
             for item in data["roles"]:
                 item = _parse_dates(item, datetime_fields=['created_at'])
                 # Ensure defaults for permissions if missing
                 item.setdefault('can_manage_users', False)
                 item.setdefault('can_manage_roles', False)
                 item.setdefault('can_manage_branches', False)
                 item.setdefault('can_view_settings', False)
                 item.setdefault('can_clear_logs', False)
                 item.setdefault('can_manage_employees', False)
                 item.setdefault('can_view_reports', False)
                 item.setdefault('can_manage_pay', False)
                 item.setdefault('can_manage_absences', False)
                 item.setdefault('can_manage_leaves', False)
                 item.setdefault('can_manage_deposits', False)
                 item.setdefault('can_manage_loans', False)
                 item.setdefault('can_manage_expenses', False) # New permission

                 db.add(Role(**item))
        await db.flush()

        if "users" in data:
            for user_data in data["users"]:
                if 'hashed_password' not in user_data or user_data['hashed_password'] is None:
                    print(f"AVERTISSEMENT: Mot de passe manquant pour {user_data.get('email', 'Utilisateur inconnu')}. Utilisation de 'password123'.")
                    user_data['hashed_password'] = hash_password("password123")
                else:
                    user_data['hashed_password'] = str(user_data['hashed_password'])

                user_data = _parse_dates(user_data, datetime_fields=['created_at'])
                user_data.setdefault('is_active', True)
                user_data.setdefault('role_id', 1)
                if user_data.get('role_id') is None:
                    print(f"AVERTISSEMENT: role_id manquant ou null pour {user_data.get('email', 'Utilisateur inconnu')}. Assignation du rôle ID 1 (Admin).")
                    user_data['role_id'] = 1
                db.add(User(**user_data))

        if "employees" in data:
            for item in data["employees"]:
                item = _parse_dates(item, datetime_fields=['created_at'])
                item.setdefault('active', True)
                item.setdefault('position', 'Inconnu')
                if item.get('branch_id') is None:
                    first_branch_res = await db.execute(select(Branch).limit(1))
                    first_branch = first_branch_res.scalar_one_or_none()
                    if first_branch:
                        print(f"AVERTISSEMENT: branch_id manquant pour employé {item.get('first_name')} {item.get('last_name')}. Assignation de la branche ID {first_branch.id}.")
                        item['branch_id'] = first_branch.id
                    else:
                        print(f"ERREUR: branch_id manquant pour employé {item.get('first_name')} {item.get('last_name')} et aucune branche par défaut trouvée. Employé ignoré.")
                        continue
                db.add(Employee(**item))
        await db.flush()

        if "attendance" in data:
            for item in data["attendance"]:
                item = _parse_dates(item, date_fields=['date'], datetime_fields=['created_at'])
                if item.get('employee_id') is None: continue
                # Convert AttendanceType
                item['atype'] = get_enum_member(AttendanceType, item.get('atype'), AttendanceType.absent)
                db.add(Attendance(**item))

        if "leaves" in data:
            for item in data["leaves"]:
                item = _parse_dates(item, date_fields=['start_date', 'end_date'], datetime_fields=['created_at'])
                if item.get('employee_id') is None: continue
                # Convert LeaveType
                item['ltype'] = get_enum_member(LeaveType, item.get('ltype'), LeaveType.unpaid)
                item.setdefault('approved', False)
                db.add(Leave(**item))

        if "deposits" in data:
            for item in data["deposits"]:
                item = _parse_dates(item, date_fields=['date'], datetime_fields=['created_at'])
                if item.get('employee_id') is None: continue
                item.setdefault('amount', 0.0)
                db.add(Deposit(**item))

        if "expenses" in data:
            for item in data["expenses"]:
                 item = _parse_dates(item, date_fields=['date'], datetime_fields=['created_at'])
                 if item.get('created_by') is None: 
                     # Fallback to first admin if missing
                     item['created_by'] = user['id'] 
                 db.add(models.Expense(**item))

        if "audit_logs" in data:
            print(f"Importation de {len(data['audit_logs'])} entrées d'audit log...") # Optional: Add logging
            for item in data["audit_logs"]:
                item = _parse_dates(item, datetime_fields=['created_at'])
                if item.get('actor_id') is None:
                    # Maybe try to find user by email if actor_id is missing but email exists?
                    # For now, we skip if actor_id is essential and missing.
                    print(f"AVERTISSEMENT: actor_id manquant pour l'entrée d'audit log ID {item.get('id', 'N/A')}. Log ignoré.")
                    continue
                # Set defaults for nullable fields if they are missing
                item.setdefault('entity_id', None)
                item.setdefault('branch_id', None)
                item.setdefault('details', None)
                # Ensure required fields like action and entity exist
                if not item.get('action') or not item.get('entity'):
                    print(f"AVERTISSEMENT: Action ou Entité manquante pour l'entrée d'audit log ID {item.get('id', 'N/A')}. Log ignoré.")
                    continue

                # Remove 'id' if present, let DB generate new one if needed, or handle potential conflicts
                item.pop('id', None)

                db.add(AuditLog(**item))

        if "pay_history" in data:
            for item in data["pay_history"]:
                item = _parse_dates(item, date_fields=['date'], datetime_fields=['created_at'])
                if item.get('employee_id') is None: continue
                # Convert PayType <<<<------ FIX IS HERE
                item['pay_type'] = get_enum_member(PayType, item.get('pay_type'), PayType.mensuel) # Assuming 'mensuel' is a valid default
                item.setdefault('amount', 0.0)
                # item.setdefault('pay_type', PayType.salary) # Incorrect default removed
                db.add(Pay(**item))

        if "loans" in data:
            for item in data["loans"]:
                item = _parse_dates(item, date_fields=['start_date', 'next_due_on'], datetime_fields=['created_at'])
                if item.get('employee_id') is None: continue
                # Convert LoanStatus and LoanTermUnit
                item['status'] = get_enum_member(LoanStatus, item.get('status'), LoanStatus.draft)
                item['term_unit'] = get_enum_member(LoanTermUnit, item.get('term_unit'), LoanTermUnit.month)
                # Convert LoanInterestType (though likely 'none' based on your code)
                item['interest_type'] = get_enum_member(LoanInterestType, item.get('interest_type'), LoanInterestType.none)

                item.setdefault('principal', 0.0)
                item.setdefault('term_count', 1)
                item.setdefault('repaid_total', 0.0)
                # Recalculate scheduled_total and outstanding_principal if needed, or use defaults
                item.setdefault('scheduled_total', item.get('principal', 0.0))
                item.setdefault('outstanding_principal', item.get('principal', 0.0) - item.get('repaid_total', 0.0))
                db.add(Loan(**item))
        await db.flush()

        if "loan_schedules" in data:
            for item in data["loan_schedules"]:
                item = _parse_dates(item, date_fields=['due_date'], datetime_fields=['created_at'])
                if item.get('loan_id') is None: continue
                # --- FIX: Use correct Enum name ScheduleStatus ---
                item['status'] = get_enum_member(ScheduleStatus, item.get('status'), ScheduleStatus.pending) # Convert Enum using correct name
                # --- END FIX ---
                item.setdefault('sequence_no', 0)
                item.setdefault('due_total', 0.0)
                item.setdefault('paid_total', 0.0)
                db.add(LoanSchedule(**item))

        if "loan_repayments" in data:
            for item in data["loan_repayments"]:
                item = _parse_dates(item, date_fields=['paid_on'], datetime_fields=['created_at'])
                if item.get('loan_id') is None: continue
                # Convert RepaymentSource
                item['source'] = get_enum_member(RepaymentSource, item.get('source'), RepaymentSource.cash)
                item.setdefault('amount', 0.0)
                db.add(LoanRepayment(**item))

        await db.commit()
        print("✅ Importation terminée avec succès.") # Success message

    except json.JSONDecodeError:
        print("ERREUR: Le fichier de sauvegarde n'est pas un JSON valide.")
        await db.rollback()
    except KeyError as e:
         print(f"ERREUR lors de l'import: Clé manquante dans le JSON - {e}")
         traceback.print_exc()
         await db.rollback()
    except Exception as e:
        await db.rollback()
        print(f"ERREUR lors de l'import: {e}")
        traceback.print_exc()

    return RedirectResponse(request.url_for('settings_page'), status_code=status.HTTP_302_FOUND)


#
# --- SECTION DES PRÊTS (WEB) ---
#

@app.get("/loans", name="loans_page")
async def loans_page(request: Request, db: AsyncSession = Depends(get_db), user: dict = Depends(web_require_permission("can_manage_loans"))):
    # Load Branches for Admin Selector
    res_branches = await db.execute(select(Branch).order_by(Branch.name))
    branches = res_branches.scalars().all()

    employees_query = select(Employee).where(Employee.active==True).order_by(Employee.first_name)
    loans_query = select(Loan).options(selectinload(Loan.employee), selectinload(Loan.creator)).order_by(Loan.start_date.desc(), Loan.created_at.desc())
    
    permissions = user.get("permissions", {})
    selected_branch_id = request.query_params.get("branch_id")

    if not permissions.get("is_admin"):
        user_branch_id = user.get("branch_id")
        employees_query = employees_query.where(Employee.branch_id == user_branch_id)
        loans_query = loans_query.join(Employee).where(Employee.branch_id == user_branch_id)
    else:
        # Admin Filter
        if selected_branch_id and selected_branch_id.isdigit():
             bid = int(selected_branch_id)
             employees_query = employees_query.where(Employee.branch_id == bid)
             loans_query = loans_query.join(Employee).where(Employee.branch_id == bid)

    employees = (await db.execute(employees_query)).scalars().all()
    loans = (await db.execute(loans_query.limit(200))).scalars().all()
    
    today_date_iso = get_tunisia_today().isoformat()

    return templates.TemplateResponse("loans.html", {
        "request": request, 
        "user": user, 
        "app_name": APP_NAME, 
        "employees": employees, 
        "loans": loans,
        "branches": branches,
        "selected_branch_id": selected_branch_id,
        "today_date": today_date_iso 
    })


@app.post("/loans/create", name="loans_create_web")
async def loans_create_web(
    request: Request,
    employee_id: Annotated[int, Form()],
    principal: Annotated[Decimal, Form()],
    term_count: Annotated[int, Form()] = 1, # Gardé pour compatibilité API
    term_unit: Annotated[str, Form()] = "month", # Gardé pour compatibilité API
    
    # --- FIX: Change parameter to be an optional Form field ---
    start_date: Annotated[dt_date | None, Form()] = None, 

    first_due_date: Annotated[dt_date | None, Form()] = None, # Gardé pour compatibilité API
    notes: Annotated[str, Form()] = None,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_loans")),
):

    # --- FIX: Add logic to set default date if not provided ---
    effective_start_date = start_date
    if effective_start_date is None:
        effective_start_date = get_tunisia_today()
    # --- END FIX ---

    # Vérifier l'autorisation de gérer l'employé
    res_emp = await db.execute(select(Employee).where(Employee.id == employee_id))
    employee = res_emp.scalar_one_or_none()
    if not employee:
         return RedirectResponse(request.url_for("loans_page"), status_code=status.HTTP_302_FOUND)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin") and user.get("branch_id") != employee.branch_id:
        return RedirectResponse(request.url_for("loans_page"), status_code=status.HTTP_302_FOUND)

    # Créer le payload pour l'API interne, même si certains champs ne sont plus utilisés par la logique web
    payload = LoanCreate(
        employee_id=employee_id, principal=principal, interest_type="none",
        annual_interest_rate=None, term_count=term_count, term_unit=term_unit,
        
        start_date=effective_start_date, # --- FIX: Use the new variable ---
        
        first_due_date=first_due_date, fee=None

    )
    from app.api.loans import create_loan
    new_loan = await create_loan(payload, db, user)

    # Ajouter la note manuellement
    if new_loan and notes:
        try:
            new_loan.notes = notes or None
            await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"Erreur lors de l'ajout de la note au prêt: {e}")

    return RedirectResponse(request.url_for("loans_page"), status_code=status.HTTP_302_FOUND)


@app.get("/loan/{loan_id}", response_class=HTMLResponse, name="loan_detail_page")
async def loan_detail_page(
    request: Request,
    loan_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_loans"))
):
    """Affiche la page de détails d'un prêt."""

    loan_query = select(Loan).options(
            selectinload(Loan.employee), selectinload(Loan.creator)
            # --- FIX: Ne pas charger les relations ici, les requêter séparément ---
            # selectinload(Loan.schedules),
            # selectinload(Loan.repayments)
            # --- FIN DU FIX ---
        ).where(Loan.id == loan_id)

    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        loan_query = loan_query.join(Employee).where(Employee.branch_id == user.get("branch_id"))

    loan = (await db.execute(loan_query)).scalar_one_or_none()

    if not loan:
        return RedirectResponse(request.url_for("loans_page"), status_code=status.HTTP_302_FOUND)

    # --- FIX: Requêtes séparées pour les relations avec tri ---
    schedules_res = await db.execute(
        select(LoanSchedule)
        .where(LoanSchedule.loan_id == loan_id)
        .order_by(LoanSchedule.due_date.desc())
    )
    schedules = schedules_res.scalars().all()

    repayments_res = await db.execute(
        select(LoanRepayment)
        .where(LoanRepayment.loan_id == loan_id)
        .order_by(LoanRepayment.paid_on.desc(), LoanRepayment.created_at.desc())
    )
    repayments = repayments_res.scalars().all()
    # --- FIN DU FIX ---

    today_date = get_tunisia_today().isoformat()

    return templates.TemplateResponse(
        "loan_detail.html",
        {
            "request": request,
            "user": user,
            "app_name": APP_NAME,
            "loan": loan,
            "schedules": schedules, # Passer les listes triées
            "repayments": repayments, # Passer les listes triées
            "today_date": today_date
        }
    )

# --- NOUVEAU : Route pour supprimer un prêt ---
@app.post("/loan/{loan_id}/delete", name="loan_delete_web")
async def loan_delete_web(
    request: Request,
    loan_id: int,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_loans")) # Ou 'is_admin'
):
    """Supprime un prêt, ses échéances et ses remboursements."""

    # Vérifier si l'utilisateur a le droit de voir/supprimer ce prêt
    loan_query = select(Loan).options(selectinload(Loan.employee)).where(Loan.id == loan_id)
    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
        loan_query = loan_query.join(Employee).where(Employee.branch_id == user.get("branch_id"))

    loan = (await db.execute(loan_query)).scalar_one_or_none()

    if loan:
        try:
            employee_id_log = loan.employee_id # Sauvegarder avant suppression
            # Vérifier si l'employé existe encore avant d'accéder à branch_id
            branch_id_log = loan.employee.branch_id if loan.employee else None

            # La suppression en cascade est gérée par app/models.py
            await db.delete(loan)
            await db.commit()

            await log(
                db, user['id'], "delete", "loan", loan_id,
                branch_id_log, f"Prêt supprimé pour l'employé ID={employee_id_log}"
            )
            await db.commit() # Commit du log
        except Exception as e:
            await db.rollback()
            print(f"Erreur lors de la suppression du prêt {loan_id}: {e}")
            import traceback
            traceback.print_exc()


    return RedirectResponse(request.url_for("loans_page"), status_code=status.HTTP_302_FOUND)
# --- FIN NOUVEAU ---

@app.post("/loan/{loan_id}/repay", name="loan_repay_web")
async def loan_repay_web(
    request: Request,
    loan_id: int,
    amount: Annotated[Decimal, Form()],
    paid_on: Annotated[dt_date, Form()],
    notes: Annotated[str, Form()] = None,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("can_manage_loans"))
):
    """Traite le formulaire de remboursement depuis la page web."""

    # Vérifier l'autorisation avant de traiter le remboursement
    loan_check_query = select(Loan).options(selectinload(Loan.employee)).where(Loan.id == loan_id)
    permissions = user.get("permissions", {})
    if not permissions.get("is_admin"):
          loan_check_query = loan_check_query.join(Employee).where(Employee.branch_id == user.get("branch_id"))

    loan_exists = (await db.execute(loan_check_query)).scalar_one_or_none()
    if not loan_exists:
        # L'utilisateur n'a pas accès à ce prêt ou il n'existe pas
         return RedirectResponse(request.url_for("loans_page"), status_code=status.HTTP_302_FOUND)

    payload = schemas.RepaymentCreate(
        amount=amount, paid_on=paid_on, source="cash",
        notes=notes or None, schedule_id=None
    )

    try:
        # L'API (repay) gère déjà la logique de paiement flexible/partiel et le log d'audit
        await loans_api.repay(loan_id=loan_id, payload=payload, db=db, user=user)
    except HTTPException as e:
        print(f"Erreur HTTP lors du remboursement web pour prêt {loan_id}: {e.detail}")
        # Ajouter potentiellement un message flash ici
    except Exception as e:
         print(f"Erreur générale lors du remboursement web pour prêt {loan_id}: {e}")
         await db.rollback() # S'assurer que la session est propre en cas d'erreur inattendue
         # Ajouter potentiellement un message flash ici

    return RedirectResponse(
        request.url_for("loan_detail_page", loan_id=loan_id),
        status_code=status.HTTP_302_FOUND
    )

# --- TEMPORARY FIX ROUTE ---
@app.get("/fix_db_schema")
async def fix_db_schema(db: AsyncSession = Depends(get_db)):
    """
    Executes necessary ALTER TABLE commands to fix the database schema
    without requiring the user to access the SQL console.
    """
    try:
        # 1. Add can_manage_expenses to roles
        await db.execute(text("ALTER TABLE roles ADD COLUMN IF NOT EXISTS can_manage_expenses BOOLEAN DEFAULT FALSE;"))
        
        # 2. Add has_cnss to employees (just in case)
        await db.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS has_cnss BOOLEAN DEFAULT FALSE;"))
        
        await db.commit()
        return {"status": "success", "message": "Database schema updated successfully. Missing columns added."}
    except Exception as e:
        return {"status": "error", "message":str(e)}
