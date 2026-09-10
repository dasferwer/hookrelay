"""Run on the host from this project directory. Restarts only this project's workers."""

import json
import subprocess
import time

import smoke

smoke.BASE = "http://localhost:8110"
smoke.RECEIVER = "http://localhost:8111"


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True)


def main():
    token = smoke.new_user()
    sid, _ = smoke.scenario(token, "recovery.crash", delay_first=1, delay_after_commit_seconds=8)
    try:
        compose("stop", "--timeout", "5", "worker", "dispatcher")
        event = smoke.emit(token, "recovery.crash")
        status = smoke.call("GET", f"/deliveries/{event['delivery_ids'][0]}", token=token)[1]
        assert status["status"] == "pending"
        compose("up", "-d", "--no-deps", "dispatcher", "worker")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            stats = smoke.call("GET", f"/control/{sid}", receiver=True)[1]
            if stats["unique_effects"] == 1:
                break
            time.sleep(0.1)
        assert stats["unique_effects"] == 1, stats
        compose("kill", "--signal", "SIGKILL", "worker")
        compose("up", "-d", "--no-deps", "worker")
        delivery = smoke.wait_delivery(token, event, timeout=45)
        stats = smoke.call("GET", f"/control/{sid}", receiver=True)[1]
        assert (
            delivery["attempts"] >= 2
            and stats["unique_effects"] == 1
            and stats["accepted_requests"] >= 2
        ), (delivery, stats)
        assert any(h["result"] == "lease_expired" for h in delivery["history"]), delivery
        print(
            json.dumps(
                {
                    "ok": True,
                    "accepted_while_workers_stopped": True,
                    "worker_killed_after_receiver_commit": True,
                    "recovered_status": delivery["status"],
                    "attempts": delivery["attempts"],
                    "unique_receiver_effects": stats["unique_effects"],
                }
            )
        )
    finally:
        compose("up", "-d", "--no-deps", "worker", "dispatcher")


if __name__ == "__main__":
    main()
