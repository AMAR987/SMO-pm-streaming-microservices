import asyncio

from utils import (
    initialize_kafka,
    handle_kafka_error,
    initialize_influx_client,
    initialize_write_api,
    write_to_influx,
    commit_kafka_message,
    close_kafka_consumer,
    close_influx_resources,
)

class MemoryBuffer:

    def __init__(self, maxsize=10000):
        self.queue = asyncio.Queue(maxsize=maxsize)

    async def put(self, message):
        await self.queue.put(message)

    async def get(self):
        return await self.queue.get()

    async def requeue(self, message):
        await self.queue.put(message)

    def task_done(self):
        self.queue.task_done()



async def kafka_consumer(memory_buffer, config):

    consumer = None

    try:
        consumer = awaitinitialize_kafka(config)

        await consumer.subscribe([
            config["kafka_topic"]
        ])

        while True:

            try:

                record = await consumer.poll(
                    timeout=config.get(
                        "poll_timeout",
                          1.0,
                    ),
                )

                if record is None:
                    continue

                if record.error():

                    handle_kafka_error(
                        record.error()
                    )

                    continue

                message_data = {
                    "message": record.value(),
                    "record": record,
                    "topic": record.topic(),
                    "partition": record.partition(),
                    "offset": record.offset(),
                }

                await memory_buffer.put(
                    message_data
                )

                await commit_kafka_message(
                    consumer,
                    message_data,
                )

            except Exception as error:

                handle_kafka_error(error)

                raise

    finally:

        await close_kafka_consumer(
            consumer
        )


async def influx_writer(memory_buffer, config):

    client = None
    write_api = None

    try:

        client = initialize_influx_client(config)

        write_api = initialize_write_api(client, config)

        while True:

            message_data = await memory_buffer.get()

            try:

                message = message_data["message"]

                success = await write_to_influx(
                    write_api,
                    message,
                    config,
                )

                if not success:

                    await memory_buffer.requeue(message_data)

                    await asyncio.sleep(
                        config.get(
                            "retry_delay",
                            5,
                        )
                    )

            except Exception as error:

                print(
                    f"Writer processing error: {error}"
                )

                await memory_buffer.requeue(
                    message_data
                )

                await asyncio.sleep(
                    config.get(
                        "retry_delay",
                        5,
                    )
                )

            finally:

                memory_buffer.task_done()

    finally:

        await close_influx_resources(
            client,
            write_api,
        )


async def supervisor(memory_buffer, config):

    kafka_task = asyncio.create_task(
        kafka_consumer(
            memory_buffer,
            config,
        )
    )

    writer_task = asyncio.create_task(
        influx_writer(
            memory_buffer,
            config,
        )
    )

    while True:

        done, pending = await asyncio.wait(
            [
                kafka_task,
                writer_task,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if kafka_task in done:

            try:
                kafka_task.result()

            except Exception as error:
                print(
                    f"Kafka Consumer stopped: {error}"
                )

            print(
                "Restarting Kafka Consumer..."
            )

            await asyncio.sleep(
                config.get(
                    "restart_delay",
                    5,
                )
            )

            kafka_task = asyncio.create_task(
                kafka_consumer(
                    memory_buffer,
                    config,
                )
            )

        if writer_task in done:

            try:
                writer_task.result()

            except Exception as error:
                print(
                    f"InfluxDB Writer stopped: {error}"
                )

            print(
                "Restarting InfluxDB Writer..."
            )

            await asyncio.sleep(
                config.get(
                    "restart_delay",
                    5,
                )
            )

            writer_task = asyncio.create_task(
                influx_writer(
                    memory_buffer,
                    config,
                )
            )


async def main(config):

    memory_buffer = MemoryBuffer(
        maxsize=config.get(
            "buffer_size",
            10000,
        )
    )

    await supervisor(
        memory_buffer,
        config,
    )