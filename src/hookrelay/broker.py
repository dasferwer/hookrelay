import aio_pika

QUEUE = "hookrelay.ready"


async def open_channel(connection):
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    await channel.set_qos(prefetch_count=8)
    queue = await channel.declare_queue(QUEUE, durable=True, arguments={"x-queue-type": "quorum"})
    return channel, queue


async def publish(channel, payload):
    import json

    await channel.default_exchange.publish(
        aio_pika.Message(
            json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        ),
        routing_key=QUEUE,
        mandatory=True,
        timeout=5,
    )
