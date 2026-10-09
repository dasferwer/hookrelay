"""Сетевые границы: локальный HTTP, настоящий receiver/БД и контролируемые DNS-ответы."""

import asyncio
import json
import socket
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import aiohttp
import httpx
import pytest
from aiohttp import web
from sqlalchemy import text

from hookrelay.config import settings
from hookrelay.db import engine
from hookrelay.receiver import app as receiver
from hookrelay.security import PublicResolver, cipher, verify_signature
from hookrelay.service import claim_delivery, finish_delivery, reserve_due
from hookrelay.worker import send


@asynccontextmanager
async def server(handler):
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    address = runner.addresses[0]
    try:
        yield f"http://127.0.0.1:{address[1]}"
    finally:
        await runner.cleanup()


def delivery(url, event=None, body=None):
    event = event or uuid4()
    secret = "network-test-signing-secret"
    return {
        "url": url,
        "event_id": event,
        "attempt_no": 1,
        "body": body or json.dumps({"id": str(event), "data": {"a": 1}}),
        "secret_ciphertext": cipher().encrypt(secret.encode()).decode(),
    }


@pytest.mark.parametrize(
    "address", ["::1", "fe80::1", "::ffff:127.0.0.1", "2001:db8::1", "ff02::1"]
)
async def test_mixed_a_aaaa_rejects_nonpublic_ipv6(monkeypatch, address):
    async def resolve(*args, **kwargs):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", (address, 443, 0, 0)),
        ]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    with pytest.raises(OSError, match="private"):
        await PublicResolver().resolve("example.com", 443, socket.AF_UNSPEC)


async def test_public_dual_stack_is_pinned_then_rebinding_is_refused(monkeypatch):
    calls = 0

    async def resolve(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
                (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:4700:4700::1111", 443, 0, 0)),
            ]
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 443))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    resolver = PublicResolver()
    first = await resolver.resolve("example.com", 443, socket.AF_UNSPEC)
    assert len(first) == 2
    assert all(value["flags"] == socket.AI_NUMERICHOST for value in first)
    with pytest.raises(OSError):
        await resolver.resolve("example.com", 443, socket.AF_UNSPEC)
    assert calls == 2


async def test_real_http_redirect_is_not_followed(monkeypatch):
    monkeypatch.setattr(settings, "allowed_insecure_hosts", ["127.0.0.1"])
    calls = []

    async def target(request):
        calls.append(request.path)
        return web.json_response({"unexpected": True})

    async with server(target) as target_url:

        async def redirect(request):
            return web.Response(status=302, headers={"Location": target_url + "/private"})

        async with server(redirect) as url, aiohttp.ClientSession(trust_env=False) as session:
            outcome = await send(session, delivery(url + "/redirect"))
    assert outcome.status_code == 302 and calls == []


async def test_timeout_after_receiver_commit_redelivers_same_body_once(monkeypatch):
    monkeypatch.setattr(settings, "allowed_insecure_hosts", ["127.0.0.1"])
    monkeypatch.setattr(settings, "request_timeout_seconds", 0.2)
    scenario = uuid4()
    job = delivery("unused")
    secret = cipher().decrypt(job["secret_ciphertext"].encode()).decode()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=receiver), base_url="http://receiver"
    ) as backend:
        configured = await backend.put(
            f"/control/{scenario}",
            json={"secret": secret, "delay_first": 1, "delay_after_commit_seconds": 0.7},
            headers={"X-Demo-Token": settings.demo_receiver_token},
        )
        assert configured.status_code == 200

        async def forward(request):
            result = await backend.post(
                request.path, content=await request.read(), headers=dict(request.headers)
            )
            return web.Response(status=result.status_code, body=result.content)

        async with server(forward) as url, aiohttp.ClientSession(trust_env=False) as session:
            job["url"] = url + f"/hooks/{scenario}"
            first = await send(session, job)
            assert first.status_code is None and first.error in {
                "TimeoutError",
                "ServerTimeoutError",
            }
            async with engine.connect() as conn:
                assert (
                    await conn.scalar(
                        text("SELECT count(*) FROM demo_received_events WHERE scenario_id=:id"),
                        {"id": scenario},
                    )
                    == 1
                )
            second = await send(session, {**job, "attempt_no": 2})
            assert second.status_code == 200
            # Другой body с тем же ID не становится новым действием приёмника.
            changed = {**job, "body": json.dumps({"id": str(job["event_id"]), "data": {"a": 2}})}
            assert (await send(session, changed)).status_code == 409
        stats = (
            await backend.get(
                f"/control/{scenario}", headers={"X-Demo-Token": settings.demo_receiver_token}
            )
        ).json()
        assert stats == {"calls": 3, "unique_effects": 1, "accepted_requests": 2}


async def test_disable_during_send_does_not_promise_retracting_request(
    client, identities, monkeypatch
):
    monkeypatch.setattr(settings, "allowed_insecure_hosts", ["127.0.0.1"])
    entered, release = asyncio.Event(), asyncio.Event()
    captured = []

    async def accept(request):
        captured.append((await request.read(), dict(request.headers)))
        entered.set()
        await release.wait()
        return web.json_response({"accepted": True})

    headers = identities["alice"]["headers"]
    async with server(accept) as url, aiohttp.ClientSession(trust_env=False) as session:
        endpoint = await client.post(
            "/endpoints",
            json={"url": url + "/hooks", "event_types": ["order.created"]},
            headers=headers,
        )
        assert endpoint.status_code == 201, endpoint.text
        event = await client.post(
            "/events",
            json={"type": "order.created", "data": {"order": 42}},
            headers={**headers, "Idempotency-Key": uuid4().hex},
        )
        assert event.status_code == 202
        message = (await reserve_due())[0]
        job = await claim_delivery(UUID(message["id"]), UUID(message["generation"]))
        sending = asyncio.create_task(send(session, job))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            disabled = await client.patch(
                f"/endpoints/{endpoint.json()['id']}", json={"enabled": False}, headers=headers
            )
            assert disabled.status_code == 200
        finally:
            release.set()
        outcome = await sending
        assert outcome.status_code == 200
        assert await finish_delivery(
            UUID(message["id"]), UUID(message["generation"]), job["attempt_id"], outcome
        )
        assert len(captured) == 1
        body, sent_headers = captured[0]
        secret = cipher().decrypt(job["secret_ciphertext"].encode()).decode()
        assert verify_signature(
            secret, sent_headers["X-Webhook-Timestamp"], body, sent_headers["X-Webhook-Signature"]
        )
        assert body == job["body"].encode()
        following = await client.post(
            "/events",
            json={"type": "order.created", "data": {"order": 43}},
            headers={**headers, "Idempotency-Key": uuid4().hex},
        )
        assert following.status_code == 202 and following.json()["delivery_ids"] == []
