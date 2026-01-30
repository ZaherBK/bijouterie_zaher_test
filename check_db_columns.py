import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

NEON_DB_URL = "postgresql+asyncpg://neondb_owner:npg_y7eFOUEX4prG@ep-weathered-recipe-agay3m4t-pooler.c-2.eu-central-1.aws.neon.tech/hrdb?ssl=require"

async def check_columns():
    engine = create_async_engine(NEON_DB_URL)
    async with engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'employees'
        """))
        columns = result.fetchall()
        print("\nColumns in 'employees' table:")
        for col in columns:
            print(f"- {col[0]} ({col[1]})")
            
        print("\nAttempting to add 'salary_frequency' column...")
        try:
            await conn.execute(text("ALTER TABLE employees ADD COLUMN salary_frequency VARCHAR(50) DEFAULT 'monthly'"))
            await conn.commit()
            print("Successfully added 'salary_frequency'.")
        except Exception as e:
            print(f"Error adding column: {e}")

    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_columns())
