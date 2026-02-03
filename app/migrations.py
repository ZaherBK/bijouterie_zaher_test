import logging
from sqlalchemy import text
from app.db import engine

logger = logging.getLogger("uvicorn")

async def run_migrations():
    """Checks for missing columns and adds them if needed."""
    logger.info("Checking for database migrations...")
    async with engine.begin() as conn:
        try:
            # --- EXPENSES MIGRATION ---
            # Check if 'branch_id' column exists safely
            result = await conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='expenses' AND column_name='branch_id'"))
            if not result.scalar():
                logger.info("Migrating: Adding branch_id to expenses table...")
                await conn.execute(text("ALTER TABLE expenses ADD COLUMN branch_id INTEGER REFERENCES branches(id)"))
                logger.info("Migration successful: branch_id added to expenses.")

            # --- EMPLOYEES MIGRATION (Salary Frequency) ---
            result = await conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='employees' AND column_name='salary_frequency'"))
            if not result.scalar():
                logger.info("Migrating: Adding salary_frequency to employees table...")
                await conn.execute(text("ALTER TABLE employees ADD COLUMN salary_frequency VARCHAR(50) DEFAULT 'monthly'"))
                logger.info("Migration successful: salary_frequency added to employees.")
            
            # --- WORK DAYS MIGRATION ---
            result = await conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='employees' AND column_name='work_days'"))
            if not result.scalar():
                logger.info("Migrating: Adding work_days to employees table...")
                # Default to Mon-Sat (0,1,2,3,4,5)
                await conn.execute(text("ALTER TABLE employees ADD COLUMN work_days VARCHAR(50) DEFAULT '0,1,2,3,4,5'"))
                logger.info("Migration successful: work_days added to employees.")
            
            # --- FORCE WEEKLY PAYROLL MIGRATION ---
            # As per user request: "all of them will be weekly"
            # We update all existing employees to 'weekly'
            await conn.execute(text("UPDATE employees SET salary_frequency = 'weekly'"))
            logger.info("Migration: All employees updated to Weekly salary frequency.")

            # --- ENUM MIGRATION (Postgres) ---
            try:
                # Add 'sick_unpaid' to LeaveType enum if not exists
                # This syntax is generic for Postgres but fails gracefully on SQLite
                await conn.execute(text("ALTER TYPE leavetype ADD VALUE IF NOT EXISTS 'sick_unpaid'"))
                logger.info("Migrated Enum: Added 'sick_unpaid' to leavetype.")
            except Exception as e:
                # Expected on SQLite or if already exists in older PG versions
                logger.info(f"Enum migration skipped (checked): {e}")

        except Exception as e:
            logger.error(f"Migration check failed: {e}")
