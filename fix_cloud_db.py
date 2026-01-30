import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from app.models import Base, SalesSummary
from app.db import Base as DbBase

# NEON DB URL Provided by User
# Note: transforming 'postgresql://' to 'postgresql+asyncpg://' for async engine
# Fix: asyncpg uses 'ssl=require', not 'sslmode=require'
NEON_DB_URL = "postgresql+asyncpg://neondb_owner:npg_y7eFOUEX4prG@ep-weathered-recipe-agay3m4t-pooler.c-2.eu-central-1.aws.neon.tech/hrdb?ssl=require"

async def fix_database():
    print("Connecting to Neon DB...")
    engine = create_async_engine(NEON_DB_URL, echo=True)
    
    async with engine.begin() as conn:
        print("Creating missing tables (SalesSummary)...")
        # specific fix: ensure metadata knows about everything
        await conn.run_sync(Base.metadata.create_all)
        
        # --- FIX: Manually Add Missing Column ---
        from sqlalchemy import text
        print("Migrating Employee Table: Adding salary_frequency column...")
        try:
            await conn.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS salary_frequency VARCHAR(50) DEFAULT 'monthly'"))
            print("Column 'salary_frequency' added successfully.")
        except Exception as e:
            print(f"Skipping column addition (might exist or error): {e}")
        # --- END FIX ---
        
    print("Done! Tables created and migrated.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(fix_database())
