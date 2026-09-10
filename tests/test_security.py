import asyncio
import hashlib
import json
import socket
import time
from uuid import uuid4

import httpx
import pytest

from hookrelay.config import settings
from hookrelay.receiver import app as receiver
from hookrelay.security import PublicResolver, signature, verify_signature
from hookrelay.worker import retry_after


def test_signature_detects_tampering_and_old_timestamps():
    secret = "test-signing-secret-value"
    timestamp = str(int(time.time()))
    body = b'{"hello":"world"}'
    signed = signature(secret, timestamp, body)
    assert verify_signature(secret, timestamp, body, signed)
    assert not verify_signature(secret, timestamp, body + b" ", signed)
    assert not verify_signature(secret, str(int(timestamp) - 301), body, signed)
    assert not verify_signature(secret, "invalid", body, signed)
    assert retry_after("3") == 3
    assert retry_after("bad") is None


async def test_dns_resolver_rejects_private_answer_even_in_mixed_response(monkeypatch):
    async def resolve(*args, **kwargs):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    with pytest.raises(OSError, match="private"):
        await PublicResolver().resolve("example.com", 443)


async def test_dns_resolver_returns_numeric_pinned_addresses(monkeypatch):
    async def resolve(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    answer = await PublicResolver().resolve("example.com", 443)
    assert answer[0]["host"] == "93.184.216.34" and answer[0]["flags"] == socket.AI_NUMERICHOST


async def test_real_receiver_signature_and_single_effect_on_redelivery():
    scenario = uuid4()
    event = uuid4()
    secret = hashlib.sha256(b"secret").hexdigest()
    body = json.dumps({"id": str(event), "data": {"a": 1}}).encode()
    timestamp = str(int(time.time()))
    headers = {
        "X-Webhook-Id": str(event),
        "X-Webhook-Timestamp": timestamp,
        "X-Webhook-Signature": signature(secret, timestamp, body),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=receiver), base_url="http://receiver"
    ) as client:
        assert (
            await client.put(
                f"/control/{scenario}",
                json={"secret": secret},
                headers={"X-Demo-Token": settings.demo_receiver_token},
            )
        ).status_code == 200
        assert (
            await client.post(
                f"/hooks/{scenario}",
                content=body,
                headers={**headers, "X-Webhook-Signature": "invalid"},
            )
        ).status_code == 401
        for _ in range(2):
            assert (
                await client.post(f"/hooks/{scenario}", content=body, headers=headers)
            ).status_code == 200
        stats = (
            await client.get(
                f"/control/{scenario}", headers={"X-Demo-Token": settings.demo_receiver_token}
            )
        ).json()
        assert stats == {"calls": 2, "unique_effects": 1, "accepted_requests": 2}
