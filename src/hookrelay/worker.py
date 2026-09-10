import asyncio
import json
import logging
import signal
import time
from contextlib import suppress
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from uuid import UUID

import aio_pika
import aiohttp

from .broker import open_channel
from .config import settings
from .db import engine
from .security import PublicResolver, cipher, signature, validate_url
from .service import Outcome, claim_delivery, finish_delivery, heartbeat

logger = logging.getLogger("delivery-worker")


def retry_after(value):
    if not value:
        return None
    try:
        return float(int(value))
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=UTC)
            return (target - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None


async def send(session, delivery):
    try:
        validate_url(delivery["url"])
        secret = cipher().decrypt(delivery["secret_ciphertext"].encode()).decode()
        body = delivery["body"].encode()
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": str(delivery["event_id"]),
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": signature(secret, timestamp, body),
            "X-Webhook-Attempt": str(delivery["attempt_no"]),
        }
        async with session.post(
            delivery["url"],
            data=body,
            headers=headers,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=settings.request_timeout_seconds),
        ) as response:
            # Ответ приёмника может быть большим или содержать личные данные; тело не сохраняем.
            await response.content.read(1024)
            return Outcome(
                status_code=response.status,
                retry_after=retry_after(response.headers.get("Retry-After")),
            )
    except ValueError:
        return Outcome(status_code=400, error="unsafe_endpoint_url")
    except (aiohttp.ClientError, TimeoutError, OSError) as error:
        return Outcome(error=type(error).__name__)


async def handle(message, session):
    try:
        payload = json.loads(message.body)
        delivery_id = UUID(payload["id"])
        generation = UUID(payload["generation"])
    except (ValueError, KeyError, TypeError):
        await message.reject(requeue=False)
        return
    try:
        async with message.process(requeue=True):
            delivery = await claim_delivery(delivery_id, generation)
            if delivery is None:
                return
            outcome = await send(session, delivery)
            await finish_delivery(delivery_id, generation, delivery["attempt_id"], outcome)
            logger.info(
                "delivery_id=%s attempt=%s status_code=%s error=%s",
                delivery_id,
                delivery["attempt_no"],
                outcome.status_code,
                outcome.error,
            )
    except Exception:
        logger.exception("delivery processing failed; database lease permits recovery")
        await asyncio.sleep(0.5)


async def pulse(stop):
    while not stop.is_set():
        try:
            async with engine.begin() as conn:
                await heartbeat(conn, "worker")
        except Exception:
            logger.exception("heartbeat failed")
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=2)


async def main():
    logging.basicConfig(level=logging.INFO)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    connection = await aio_pika.connect_robust(settings.amqp_url)
    tasks = set()
    pulser = asyncio.create_task(pulse(stop))
    try:
        _, queue = await open_channel(connection)
        connector = aiohttp.TCPConnector(
            resolver=PublicResolver(), use_dns_cache=False, force_close=True, limit=8
        )
        async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:

            async def consume():
                async with queue.iterator() as messages:
                    async for message in messages:
                        task = asyncio.create_task(handle(message, session))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)

            consumer = asyncio.create_task(consume())
            stopper = asyncio.create_task(stop.wait())
            try:
                completed, _ = await asyncio.wait(
                    (consumer, stopper), return_when=asyncio.FIRST_COMPLETED
                )
                if consumer in completed and not stop.is_set():
                    consumer.result()
                    raise RuntimeError("RabbitMQ consumer exited unexpectedly")
            finally:
                stop.set()
                consumer.cancel()
                stopper.cancel()
                with suppress(asyncio.CancelledError):
                    await consumer
                with suppress(asyncio.CancelledError):
                    await stopper
                if tasks:
                    await asyncio.gather(*tasks)

    finally:
        stop.set()
        await pulser
        await connection.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
