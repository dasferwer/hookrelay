"""Проверяем доставку от API через очередь до приёмника, который проверяет подпись."""

import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

BASE = os.environ.get("BASE_URL", "http://localhost:8000")
RECEIVER = os.environ.get("RECEIVER_URL", "http://receiver:8001")
CONTROL = os.environ.get("DEMO_RECEIVER_TOKEN", "local-receiver-control")


def call(method, path, data=None, token=None, key=None, receiver=False):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if key:
        headers["Idempotency-Key"] = key
    if receiver:
        headers["X-Demo-Token"] = CONTROL
    request = Request(
        (RECEIVER if receiver else BASE) + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=20) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def new_user():
    credentials = {"email": f"hooks-{uuid4().hex}@example.com", "password": "SmokePassword123!"}
    assert call("POST", "/auth/register", credentials)[0] == 201
    return call("POST", "/auth/login", credentials)[1]["access_token"]


def scenario(token, event_type, **faults):
    sid = uuid4().hex
    status, endpoint = call(
        "POST",
        "/endpoints",
        {"url": f"http://receiver:8001/hooks/{sid}", "event_types": [event_type]},
        token,
    )
    assert status == 201, endpoint
    status, result = call(
        "PUT", f"/control/{sid}", {"secret": endpoint["signing_secret"], **faults}, receiver=True
    )
    assert status == 200, result
    return sid, endpoint


def emit(token, event_type):
    key = uuid4().hex
    payload = {"type": event_type, "data": {"order_id": uuid4().hex, "amount_units": 1250}}
    status, event = call("POST", "/events", payload, token, key)
    assert status == 202 and len(event["delivery_ids"]) == 1, event
    assert call("POST", "/events", payload, token, key) == (200, event)
    return event


def wait_delivery(token, event, status="delivered", timeout=35):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        code, delivery = call("GET", f"/deliveries/{event['delivery_ids'][0]}", token=token)
        assert code == 200, delivery
        if delivery["status"] == status:
            return delivery
        time.sleep(0.2)
    raise AssertionError(delivery)


def main():
    token = new_user()
    sid, _ = scenario(token, "smoke.transient", fail_first=2)
    event = emit(token, "smoke.transient")
    delivered = wait_delivery(token, event)
    assert [h["status_code"] for h in delivered["history"]] == [503, 503, 200], delivered
    stats = call("GET", f"/control/{sid}", receiver=True)[1]
    assert stats["unique_effects"] == 1 and stats["calls"] == 3, stats

    sid, endpoint = scenario(token, "smoke.dead", success_status=400)
    event = emit(token, "smoke.dead")
    dead = wait_delivery(token, event, "dead")
    assert dead["attempts"] == 1, dead
    call("PUT", f"/control/{sid}", {"secret": endpoint["signing_secret"]}, receiver=True)
    assert call("POST", f"/deliveries/{event['delivery_ids'][0]}/replay", token=token)[0] == 200
    replayed = wait_delivery(token, event)
    assert replayed["attempts"] == 2

    sid, _ = scenario(token, "smoke.ambiguous", delay_first=1, delay_after_commit_seconds=4)
    event = emit(token, "smoke.ambiguous")
    ambiguous = wait_delivery(token, event)
    stats = call("GET", f"/control/{sid}", receiver=True)[1]
    assert (
        ambiguous["attempts"] == 2
        and stats["accepted_requests"] == 2
        and stats["unique_effects"] == 1
    ), (ambiguous, stats)
    print(
        json.dumps(
            {
                "ok": True,
                "retry_statuses": [503, 503, 200],
                "dead_letter_replay": "verified",
                "timeout_after_effect": "verified",
                "duplicate_requests": 2,
                "unique_effects": 1,
            }
        )
    )


if __name__ == "__main__":
    main()
