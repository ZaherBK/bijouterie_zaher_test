"""
Utility script to create a new user in the HR Sync application.

Usage:
    python create_user.py email "Full Name" password [role] [branch_id]

The default role is "manager" and branch_id is optional. The script connects
to the database configured in the application's environment variables and
inserts a new user record with the hashed password. It refuses to create
duplicate users with the same email.
"""
import asyncio
import sys
from sqlalchemy import select

from app.db import AsyncSessionLocal
from app.auth import hash_password
from app.models import User, Role


async def create_user(email: str, full_name: str, password: str, role: str = "manager", branch_id: int | None = None) -> None:
    """Create a new user if the email doesn't already exist."""
    async with AsyncSessionLocal() as session:
        # Check for existing user
        res = await session.execute(select(User).where(User.email == email))
        if res.scalars().first():
            print(f"User already exists: {email}")
            return
        user = User(
            email=email,
            full_name=full_name,
            hashed_password=hash_password(password),
            role=Role(role),
            branch_id=branch_id,
            is_active=True,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        print(f"Created user {user.email} (id={user.id})")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python create_user.py email \"Full Name\" password [role] [branch_id]")
        raise SystemExit(1)
    email_arg = sys.argv[1]
    full_name_arg = sys.argv[2]
    password_arg = sys.argv[3]
    role_arg = sys.argv[4] if len(sys.argv) > 4 else "manager"
    branch_id_arg = int(sys.argv[5]) if len(sys.argv) > 5 else None
    asyncio.run(create_user(email_arg, full_name_arg, password_arg, role_arg, branch_id_arg))