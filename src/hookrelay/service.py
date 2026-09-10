import hashlib
import json
import random
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import text

from .config import settings
from .db import engine


async def publish_event(user_id, event_type, payload, key):
    try:
        canonical = json.dumps(
            {"type": event_type, "data": payload},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        encoded = canonical.encode()
    except (ValueError, UnicodeEncodeError):
        raise HTTPException(
            422, "Event must contain valid finite JSON values and Unicode"
        ) from None
    if len(encoded) > 65536:
        raise HTTPException(413, "Event payload exceeds 64 KiB")
    fingerprint = hashlib.sha256(encoded).hexdigest()
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
            {"key": f"{user_id}:{key}"},
        )
        previous = (
            (
                await conn.execute(
                    text(
                        "SELECT id,request_hash FROM events WHERE user_id=:user AND idempotency_key=:key"
                    ),
                    {"user": user_id, "key": key},
                )
            )
            .mappings()
            .first()
        )
        if previous:
            if previous["request_hash"] != fingerprint:
                raise HTTPException(409, "Idempotency key was used with another payload")
            event_id = previous["id"]
        else:
            event_id = uuid4()
            now = await conn.scalar(text("SELECT clock_timestamp()"))
            body = json.dumps(
                {
                    "id": str(event_id),
                    "type": event_type,
                    "created_at": now.isoformat(),
                    "data": payload,
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            await conn.execute(
                text(
                    "INSERT INTO events(id,user_id,type,body,idempotency_key,request_hash) VALUES (:id,:user,:type,:body,:key,:hash)"
                ),
                {
                    "id": event_id,
                    "user": user_id,
                    "type": event_type,
                    "body": body,
                    "key": key,
                    "hash": fingerprint,
                },
            )
            endpoints = (
                (
                    await conn.execute(
                        text(
                            "SELECT id FROM endpoints WHERE user_id=:user AND enabled AND event_types @> CAST(:types AS jsonb)"
                        ),
                        {"user": user_id, "types": json.dumps([event_type])},
                    )
                )
                .scalars()
                .all()
            )
            for endpoint in endpoints:
                await conn.execute(
                    text(
                        "INSERT INTO deliveries(id,event_id,endpoint_id) VALUES (:id,:event,:endpoint)"
                    ),
                    {"id": uuid4(), "event": event_id, "endpoint": endpoint},
                )
        delivery_ids = (
            (
                await conn.execute(
                    text("SELECT id FROM deliveries WHERE event_id=:id ORDER BY id"),
                    {"id": event_id},
                )
            )
            .scalars()
            .all()
        )
        return {"id": event_id, "delivery_ids": delivery_ids}, bool(previous)


async def reserve_due(limit=50):
    """Резервируем доставки. Если публикация сорвётся, после lease их можно забрать снова."""
    claimed = []
    async with engine.begin() as conn:
        rows = (
            (
                await conn.execute(
                    text("""
            SELECT d.*,e.enabled FROM deliveries d JOIN endpoints e ON e.id=d.endpoint_id
            WHERE (d.status IN ('pending','retry') AND d.not_before<=clock_timestamp())
            OR (d.status IN ('queued','processing') AND d.lease_until<=clock_timestamp())
            ORDER BY d.not_before,d.id LIMIT :limit FOR UPDATE OF d SKIP LOCKED
        """),
                    {"limit": limit},
                )
            )
            .mappings()
            .all()
        )
        for row in rows:
            now = await conn.scalar(text("SELECT clock_timestamp()"))
            if row["status"] == "processing":
                await conn.execute(
                    text(
                        "UPDATE attempts SET result='lease_expired',error='worker_lease_expired',finished_at=:now WHERE delivery_id=:id AND result='running'"
                    ),
                    {"now": now, "id": row["id"]},
                )
            if not row["enabled"] or row["cycle_attempts"] >= settings.max_attempts:
                status = "cancelled" if not row["enabled"] else "dead"
                await conn.execute(
                    text(
                        "UPDATE deliveries SET status=:status,lease_until=NULL,last_error=:error,updated_at=:now WHERE id=:id"
                    ),
                    {
                        "status": status,
                        "error": "endpoint_disabled"
                        if status == "cancelled"
                        else "attempt_budget_exhausted",
                        "now": now,
                        "id": row["id"],
                    },
                )
                continue
            generation = uuid4()
            await conn.execute(
                text(
                    "UPDATE deliveries SET status='queued',generation=:generation,lease_until=:lease,updated_at=:now WHERE id=:id"
                ),
                {
                    "generation": generation,
                    "lease": now + timedelta(seconds=settings.lease_seconds),
                    "now": now,
                    "id": row["id"],
                },
            )
            claimed.append({"id": str(row["id"]), "generation": str(generation)})
        await heartbeat(conn, "dispatcher")
    return claimed


async def heartbeat(conn, name):
    await conn.execute(
        text(
            "INSERT INTO worker_heartbeats(name) VALUES (:name) ON CONFLICT(name) DO UPDATE SET seen_at=clock_timestamp()"
        ),
        {"name": name},
    )


async def claim_delivery(delivery_id, generation):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("""
            SELECT d.*,e.url,e.secret_ciphertext,e.enabled,v.body FROM deliveries d
            JOIN endpoints e ON e.id=d.endpoint_id JOIN events v ON v.id=d.event_id
            WHERE d.id=:id FOR UPDATE OF d
        """),
                    {"id": delivery_id},
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["status"] != "queued" or row["generation"] != generation:
            return None
        now = await conn.scalar(text("SELECT clock_timestamp()"))
        if row["lease_until"] <= now:
            return None
        if not row["enabled"]:
            await conn.execute(
                text(
                    "UPDATE deliveries SET status='cancelled',lease_until=NULL,last_error='endpoint_disabled' WHERE id=:id"
                ),
                {"id": delivery_id},
            )
            return None
        attempt_id = uuid4()
        attempt_no = row["attempts"] + 1
        await conn.execute(
            text("""
            UPDATE deliveries SET status='processing',attempts=attempts+1,cycle_attempts=cycle_attempts+1,
            lease_until=:lease,updated_at=:now WHERE id=:id
        """),
            {
                "lease": now + timedelta(seconds=settings.request_timeout_seconds + 5),
                "now": now,
                "id": delivery_id,
            },
        )
        await conn.execute(
            text("INSERT INTO attempts(id,delivery_id,attempt_no) VALUES (:id,:delivery,:number)"),
            {"id": attempt_id, "delivery": delivery_id, "number": attempt_no},
        )
        return {**dict(row), "attempt_id": attempt_id, "attempt_no": attempt_no}


@dataclass(frozen=True)
class Outcome:
    status_code: int | None = None
    error: str | None = None
    retry_after: float | None = None


async def finish_delivery(delivery_id, generation, attempt_id, outcome: Outcome):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM deliveries WHERE id=:id FOR UPDATE"), {"id": delivery_id}
                )
            )
            .mappings()
            .one()
        )
        if row["status"] != "processing" or row["generation"] != generation:
            return False
        success = outcome.status_code is not None and 200 <= outcome.status_code < 300
        transient = (
            outcome.status_code is None
            or outcome.status_code in (408, 429)
            or outcome.status_code >= 500
        )
        status = (
            "delivered"
            if success
            else (
                "retry" if transient and row["cycle_attempts"] < settings.max_attempts else "dead"
            )
        )
        delay = min(
            settings.max_retry_seconds,
            settings.retry_base_seconds * 2 ** (row["cycle_attempts"] - 1),
        )
        delay = min(settings.max_retry_seconds, delay * random.uniform(1, 1.25))
        if outcome.retry_after is not None:
            delay = max(delay, min(settings.max_retry_seconds, max(0, outcome.retry_after)))
        now = await conn.scalar(text("SELECT clock_timestamp()"))
        error = None if success else (outcome.error or f"http_{outcome.status_code}")
        await conn.execute(
            text("""UPDATE deliveries SET status=:status,last_error=:error,not_before=:due,
                                  lease_until=NULL,updated_at=:now WHERE id=:id"""),
            {
                "status": status,
                "error": error,
                "due": now + timedelta(seconds=delay),
                "now": now,
                "id": delivery_id,
            },
        )
        await conn.execute(
            text(
                "UPDATE attempts SET result=:status,status_code=:code,error=:error,finished_at=:now WHERE id=:id"
            ),
            {
                "status": status,
                "code": outcome.status_code,
                "error": error,
                "now": now,
                "id": attempt_id,
            },
        )
        await heartbeat(conn, "worker")
        return True
