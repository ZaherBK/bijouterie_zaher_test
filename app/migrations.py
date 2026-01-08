import logging
from sqlalchemy import text
from app.db import engine

logger = logging.getLogger("uvicorn")

async def run_migrations():
    """Checks for missing columns and adds them if needed."""
    logger.info("Checking for database migrations...")
    async with engine.begin() as conn:
        try:
            # Check if 'branch_id' exists in 'expenses'
            # Note: PRAGMA table_info is SQLite specific. For MySQL we might need different logic.
            # But since we use SQLAlchemy text(), we can try a SELECT limit 0.
            
            # --- EXPENSES MIGRATION ---
            try:
                await conn.execute(text("SELECT branch_id FROM expenses LIMIT 1"))
            except Exception:
                logger.info("Migrating: Adding branch_id to expenses table...")
                await conn.execute(text("ALTER TABLE expenses ADD COLUMN branch_id INTEGER REFERENCES branches(id)"))
                logger.info("Migration successful: branch_id added to expenses.")

        except Exception as e:
            logger.error(f"Migration check failed: {e}")
