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



async def kafka_consumer(memory_buffer, commit_queue, config):

    consumer = None

    try:
        consumer = initialize_kafka(config)

        consumer.subscribe([
            config["kafka_topic"]
        ])

        while True:

            try:

                record = await asyncio.to_thread(
                    consumer.poll,
                    config.get(
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

                while not commit_queue.empty():

                    commit_data = await commit_queue.get()

                    try:

                        await commit_kafka_message(
                            consumer,
                            commit_data,
                        )

                    except Exception as error:

                        print(
                            f"Kafka commit error: {error}"
                        )

                        await commit_queue.put(
                            commit_data
                        )

                    finally:

                        commit_queue.task_done()

            except Exception as error:

                handle_kafka_error(error)

                raise

    finally:

        await close_kafka_consumer(
            consumer
        )


async def influx_writer(memory_buffer, commit_queue, config):

    client = None
    write_api = None

    try:

        client = initialize_influx_client(config)

        write_api = initialize_write_api(client)

        while True:

            message_data = await memory_buffer.get()

            try:

                message = message_data["message"]

                success = await write_to_influx(
                    write_api,
                    message,
                    config,
                )

                if success:

                    await commit_queue.put(
                        message_data
                    )

                else:

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


async def supervisor(memory_buffer, commit_queue, config):

    kafka_task = asyncio.create_task(
        kafka_consumer(
            memory_buffer,
            commit_queue,
            config,
        )
    )

    writer_task = asyncio.create_task(
        influx_writer(
            memory_buffer,
            commit_queue,
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
                    commit_queue,
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
                    commit_queue,
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

    commit_queue = asyncio.Queue()

    await supervisor(
        memory_buffer,
        commit_queue,
        config,
    )