import asyncio
import json
from uuid import UUID, uuid4

from sqlalchemy import text

from hookrelay.config import settings
from hookrelay.db import engine
from hookrelay.service import Outcome, claim_delivery, finish_delivery, reserve_due


def headers(identities, key=None, user="alice"):
    return {**identities[user]["headers"], "Idempotency-Key": key or uuid4().hex}


async def prepare(client, identities):
    endpoint = await client.post(
        "/endpoints",
        json={"url": "https://example.com/hooks", "event_types": ["order.created"]},
        headers=headers(identities),
    )
    assert endpoint.status_code == 201, endpoint.text
    event = await client.post(
        "/events",
        json={"type": "order.created", "data": {"order": 42}},
        headers=headers(identities),
    )
    assert event.status_code == 202, event.text
    return endpoint.json(), event.json()


async def begin_attempt():
    messages = await reserve_due()
    assert len(messages) == 1
    message = messages[0]
    delivery = await claim_delivery(UUID(message["id"]), UUID(message["generation"]))
    assert delivery
    return message, delivery


async def due_now():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE deliveries SET not_before=clock_timestamp()-interval '1 second',lease_until=clock_timestamp()-interval '1 second'"
            )
        )


async def test_concurrent_publish_is_deduplicated(client, identities):
    await client.post(
        "/endpoints",
        json={"url": "https://example.com/hooks", "event_types": ["order.created"]},
        headers=headers(identities),
    )
    results = await asyncio.gather(
        *[
            client.post(
                "/events",
                json={"type": "order.created", "data": {"a": 1}},
                headers=headers(identities, "fixed"),
            )
            for _ in range(20)
        ]
    )
    assert sum(r.status_code == 202 for r in results) == 1
    assert all(r.status_code in (200, 202) for r in results)
    assert len({r.json()["id"] for r in results}) == 1
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM deliveries")) == 1
    changed = await client.post(
        "/events",
        json={"type": "order.created", "data": {"a": 2}},
        headers=headers(identities, "fixed"),
    )
    assert changed.status_code == 409


async def test_subscriptions_owner_isolation_and_secret_redaction(client, identities):
    endpoint, event = await prepare(client, identities)
    listing = (await client.get("/endpoints", headers=headers(identities))).json()
    assert (
        len(listing) == 1
        and "signing_secret" not in listing[0]
        and "secret_ciphertext" not in listing[0]
    )
    assert (await client.get("/deliveries", headers=headers(identities, user="bob"))).json() == []
    assert (
        await client.get(
            f"/deliveries/{event['delivery_ids'][0]}", headers=headers(identities, user="bob")
        )
    ).status_code == 404
    assert (
        await client.patch(
            f"/endpoints/{endpoint['id']}",
            json={"enabled": False},
            headers=headers(identities, user="bob"),
        )
    ).status_code == 404
    unrelated = await client.post(
        "/events", json={"type": "order.cancelled", "data": {}}, headers=headers(identities)
    )
    assert unrelated.json()["delivery_ids"] == []
    async with engine.connect() as conn:
        ciphertext = await conn.scalar(text("SELECT secret_ciphertext FROM endpoints"))
        assert endpoint["signing_secret"] not in ciphertext


async def test_duplicate_message_does_not_start_two_attempts(client, identities):
    await prepare(client, identities)
    messages = await reserve_due()
    message = messages[0]
    rows = await asyncio.gather(
        *[claim_delivery(UUID(message["id"]), UUID(message["generation"])) for _ in range(10)]
    )
    assert sum(row is not None for row in rows) == 1
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM attempts")) == 1


async def test_lost_publish_recovers_and_stale_message_is_ignored(client, identities):
    await prepare(client, identities)
    first = (await reserve_due())[0]  # simulate commit followed by publisher process failure
    assert await reserve_due() == []
    await due_now()
    second = (await reserve_due())[0]
    assert first["id"] == second["id"] and first["generation"] != second["generation"]
    assert await claim_delivery(UUID(first["id"]), UUID(first["generation"])) is None
    assert await claim_delivery(UUID(second["id"]), UUID(second["generation"])) is not None


async def test_worker_crash_and_stale_completion_cannot_overwrite_retry(client, identities):
    await prepare(client, identities)
    old, attempt = await begin_attempt()
    await due_now()
    new, next_attempt = await begin_attempt()
    assert not await finish_delivery(
        UUID(old["id"]), UUID(old["generation"]), attempt["attempt_id"], Outcome(status_code=200)
    )
    assert await finish_delivery(
        UUID(new["id"]),
        UUID(new["generation"]),
        next_attempt["attempt_id"],
        Outcome(status_code=200),
    )
    async with engine.connect() as conn:
        results = (
            (await conn.execute(text("SELECT result FROM attempts ORDER BY attempt_no")))
            .scalars()
            .all()
        )
        assert results == ["lease_expired", "delivered"]
        assert await conn.scalar(text("SELECT attempts FROM deliveries")) == 2


async def test_transient_failure_retries_then_succeeds(client, identities):
    _, event = await prepare(client, identities)
    first, attempt = await begin_attempt()
    await finish_delivery(
        UUID(first["id"]),
        UUID(first["generation"]),
        attempt["attempt_id"],
        Outcome(status_code=503, retry_after=5),
    )
    response = (
        await client.get(f"/deliveries/{event['delivery_ids'][0]}", headers=headers(identities))
    ).json()
    assert response["status"] == "retry" and response["last_error"] == "http_503"
    assert await reserve_due() == []
    await due_now()
    second, attempt = await begin_attempt()
    await finish_delivery(
        UUID(second["id"]),
        UUID(second["generation"]),
        attempt["attempt_id"],
        Outcome(status_code=204),
    )
    response = (
        await client.get(f"/deliveries/{event['delivery_ids'][0]}", headers=headers(identities))
    ).json()
    assert response["status"] == "delivered" and response["attempts"] == 2
    assert [row["status_code"] for row in response["history"]] == [503, 204]


async def test_attempt_budget_and_manual_replay_preserve_event(client, identities):
    _, event = await prepare(client, identities)
    for _ in range(settings.max_attempts):
        message, attempt = await begin_attempt()
        await finish_delivery(
            UUID(message["id"]),
            UUID(message["generation"]),
            attempt["attempt_id"],
            Outcome(error="TimeoutError"),
        )
        await due_now()
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT status FROM deliveries")) == "dead"
        body = await conn.scalar(text("SELECT body FROM events"))
    assert await reserve_due() == []
    response = await client.post(
        f"/deliveries/{event['delivery_ids'][0]}/replay", headers=headers(identities)
    )
    assert response.status_code == 200
    message, attempt = await begin_attempt()
    assert attempt["body"] == body and json.loads(body)["id"] == event["id"]
    await finish_delivery(
        UUID(message["id"]),
        UUID(message["generation"]),
        attempt["attempt_id"],
        Outcome(status_code=200),
    )
    async with engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT attempts FROM deliveries")) == settings.max_attempts + 1
        )
        assert await conn.scalar(text("SELECT cycle_attempts FROM deliveries")) == 1


async def test_permanent_error_is_dead_without_retry(client, identities):
    _, event = await prepare(client, identities)
    message, attempt = await begin_attempt()
    await finish_delivery(
        UUID(message["id"]),
        UUID(message["generation"]),
        attempt["attempt_id"],
        Outcome(status_code=400),
    )
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT status FROM deliveries")) == "dead"
    assert await reserve_due() == []
    assert (
        await client.post(
            f"/deliveries/{event['delivery_ids'][0]}/replay",
            headers=headers(identities, user="bob"),
        )
    ).status_code == 404


async def test_disabled_endpoint_cancels_pending_and_blocks_replay(client, identities):
    endpoint, event = await prepare(client, identities)
    await client.patch(
        f"/endpoints/{endpoint['id']}", json={"enabled": False}, headers=headers(identities)
    )
    assert await reserve_due() == []
    response = (
        await client.get(f"/deliveries/{event['delivery_ids'][0]}", headers=headers(identities))
    ).json()
    assert response["status"] == "cancelled"
    assert (
        await client.post(
            f"/deliveries/{event['delivery_ids'][0]}/replay", headers=headers(identities)
        )
    ).status_code == 409


async def test_large_payload_and_invalid_urls_are_rejected(client, identities):
    for url in (
        "http://example.com/hook",
        "https://127.0.0.1/",
        "https://[::1]/",
        "https://169.254.169.254/",
        "https://224.0.0.1/",
        "https://[ff02::1]/",
        "https://user:pass@example.com/",
        "https://example.com:5432/",
    ):
        assert (
            await client.post(
                "/endpoints",
                json={"url": url, "event_types": ["order.created"]},
                headers=headers(identities),
            )
        ).status_code == 422
    response = await client.post(
        "/events",
        json={"type": "order.created", "data": {"text": "x" * 66000}},
        headers=headers(identities),
    )
    assert response.status_code == 413


async def test_non_finite_json_is_rejected_without_creating_event(client, identities):
    response = await client.post(
        "/events",
        content='{"type":"order.created","data":{"value":NaN}}',
        headers={**headers(identities), "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM events")) == 0
