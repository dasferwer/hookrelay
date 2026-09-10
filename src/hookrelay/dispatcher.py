import asyncio
import logging
import signal
from contextlib import suppress

import aio_pika

from .broker import open_channel, publish
from .config import settings
from .db import engine
from .service import reserve_due

logger = logging.getLogger("dispatcher")


async def dispatch_once(channel):
    due = await reserve_due()
    for payload in due:
        await publish(channel, payload)
    return len(due)


async def main():
    logging.basicConfig(level=logging.INFO)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    connection = await aio_pika.connect_robust(settings.amqp_url)
    try:
        channel, _ = await open_channel(connection)
        while not stop.is_set():
            try:
                count = await dispatch_once(channel)
                if count:
                    logger.info("published=%s", count)
            except Exception:
                logger.exception("publish failed; unconfirmed claims recover after lease expiry")
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.worker_interval)
    finally:
        await connection.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
