import asyncio
from uuid import UUID

from sqlalchemy import text

from hookrelay.auth import hasher
from hookrelay.db import engine


async def seed():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users(id,email,password_hash) VALUES (:id,'demo@example.com',:hash) ON CONFLICT(email) DO NOTHING"
            ),
            {
                "id": UUID("11000000-0000-0000-0000-000000000001"),
                "hash": hasher.hash("HookRelayDemo123!"),
            },
        )
    await engine.dispose()
    print("Seed ready: demo@example.com / HookRelayDemo123!")


if __name__ == "__main__":
    asyncio.run(seed())
