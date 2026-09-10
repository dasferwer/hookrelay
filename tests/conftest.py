from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import jwt
import pytest
from sqlalchemy import text

from hookrelay.config import settings
from hookrelay.db import engine
from hookrelay.main import app

assert settings.testing and settings.database_url.endswith("_test"), (
    "Tests require an isolated *_test database"
)


@pytest.fixture(autouse=True)
async def clean():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE users,endpoints,events,deliveries,attempts,worker_heartbeats,demo_received_events RESTART IDENTITY CASCADE"
            )
        )
    yield


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest.fixture
async def identities():
    users = {}
    async with engine.begin() as conn:
        for role in ("admin", "alice", "bob"):
            uid = uuid4()
            await conn.execute(
                text(
                    "INSERT INTO users(id,email,password_hash,role) VALUES (:id,:email,:hash,:role)"
                ),
                {
                    "id": uid,
                    "email": f"{role}@example.com",
                    "hash": "unused",
                    "role": "admin" if role == "admin" else "user",
                },
            )
            now = datetime.now(UTC)
            token = jwt.encode(
                {
                    "sub": str(uid),
                    "iat": now,
                    "exp": now + timedelta(hours=1),
                    "iss": "hookrelay",
                    "aud": "hookrelay",
                },
                settings.jwt_secret,
                algorithm="HS256",
            )
            users[role] = {"id": uid, "headers": {"Authorization": f"Bearer {token}"}}
    return users


@pytest.fixture(scope="session", autouse=True)
async def dispose_pool():
    yield
    await engine.dispose()
