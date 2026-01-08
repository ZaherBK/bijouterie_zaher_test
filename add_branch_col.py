from app.db import engine
from sqlalchemy import text
import asyncio

async def add_column():
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE expenses ADD COLUMN branch_id INTEGER REFERENCES branches(id)"))
            print("Successfully added branch_id column to expenses table.")
        except Exception as e:
            print(f"Error adding column (might already exist): {e}")

if __name__ == "__main__":
    asyncio.run(add_column())
