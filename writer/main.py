import asyncio
from collections import deque

from config import WRITER_CONFIG
from util import (
    MemoryBuffer,
    initialize_kafka,
    handle_kafka_error,
    initialize_influx_client,
    initialize_write_api,
    write_to_influx,
    commit_kafka_message,
    close_kafka_consumer,
    close_influx_resources,
)


class CommitState:

    def __init__(self):
        self.pending_acks = deque()
        self.last_committed_next = {}


async def collect_pending_acks(ack_queue, commit_state):
    while True:
        try:
            ack = ack_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        commit_state.pending_acks.append(ack)
        ack_queue.task_done()


async def get_message_batch(memory_buffer, batch_size):
    batch = [await memory_buffer.get()]

    while len(batch) < batch_size:
        try:
            message_data = memory_buffer.queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        batch.append(message_data)

    return batch


async def write_batch_with_retry(write_api, batch, config):
    messages = [item["message"] for item in batch]

    while True:
        success = await write_to_influx(
            write_api,
            messages,
            config,
        )

        if success:
            return

        print(
            f"[INFLUX] Batch write failed for "
            f"{len(batch)} messages. Retrying...",
            flush=True,
        )

        await asyncio.sleep(config.get("retry_delay", 5))


async def kafka_consumer(memory_buffer, ack_queue, commit_state, config):
    consumer = None

    try:
        print("[KAFKA] Initializing consumer...", flush=True)
        consumer = initialize_kafka(config)
        await consumer.subscribe([config["kafka_topic"]])

        print(
            f"[KAFKA] Subscribed to {config['kafka_topic']}",
            flush=True,
        )

        while True:

            await collect_pending_acks(ack_queue, commit_state)

            if commit_state.pending_acks:
                ack = commit_state.pending_acks[0]
                key = (ack["topic"], ack["partition"])
                next_offset = ack["offset"] + 1

                last_committed = (
                    commit_state.last_committed_next.get(key, -1)
                )

                if next_offset <= last_committed:
                    commit_state.pending_acks.popleft()
                    continue

                try:
                    await commit_kafka_message(consumer, ack)

                    commit_state.last_committed_next[key] = next_offset
                    commit_state.pending_acks.popleft()

                    print(
                        f"[N-WRITER] Acknowledged and committed "
                        f"topic={ack['topic']} "
                        f"partition={ack['partition']} "
                        f"offset={ack['offset']}",
                        flush=True,
                    )

                except asyncio.CancelledError:
                    raise

                except Exception as error:
                    print(
                        f"[KAFKA] Commit failed for "
                        f"topic={ack['topic']} "
                        f"partition={ack['partition']} "
                        f"offset={ack['offset']}: {error}. "
                        f"Retrying commit.",
                        flush=True,
                    )
                    await asyncio.sleep(
                        config.get("retry_delay", 5)
                    )

                continue

            try:
                record = await consumer.poll(
                    timeout=config.get("poll_timeout", 1.0)
                )

                if record is None:
                    continue

                if record.error():
                    handle_kafka_error(record.error())
                    continue

                message_data = {
                    "message": record.value().decode("utf-8"),
                    "topic": record.topic(),
                    "partition": record.partition(),
                    "offset": record.offset(),
                }

                print(
                    f"[KAFKA] Received "
                    f"topic={record.topic()} "
                    f"partition={record.partition()} "
                    f"offset={record.offset()}",
                    flush=True,
                )

                await memory_buffer.put(message_data)

            except asyncio.CancelledError:
                raise

            except Exception as error:
                handle_kafka_error(error)
                raise

    finally:
        await close_kafka_consumer(consumer)


async def influx_writer(memory_buffer, ack_queue, config):
    client = None
    write_api = None

    try:
        print("[INFLUX] Initializing client...", flush=True)

        client = initialize_influx_client(config)
        write_api = initialize_write_api(client, config)

        print("[INFLUX] Writer initialized.", flush=True)

        batch_size = config.get("batch_size", 100)

        while True:
            batch = await get_message_batch(
                memory_buffer,
                batch_size,
            )


            try:
                await write_batch_with_retry(
                    write_api,
                    batch,
                    config,
                )

                print(
                    f"[INFLUX] Batch write succeeded: "
                    f"{len(batch)} messages",
                    flush=True,
                )

                for item in batch:
                    await ack_queue.put({
                        "topic": item["topic"],
                        "partition": item["partition"],
                        "offset": item["offset"],
                    })


            except asyncio.CancelledError:
                raise

            finally:
                for _ in batch:
                    memory_buffer.task_done()

    finally:
        await close_influx_resources(client, write_api)

async def supervisor(memory_buffer, ack_queue, commit_state, config):
    restart_delay = config.get("restart_delay", 5)
    health_interval = config.get("health_check_interval", 10)

    kafka_task = asyncio.create_task(
        kafka_consumer(
            memory_buffer, ack_queue, commit_state, config
        ),
        name="kafka_consumer",
    )

    writer_task = asyncio.create_task(
        influx_writer(memory_buffer, ack_queue, config),
        name="influx_writer",
    )

    while True:
        done, _ = await asyncio.wait(
            [kafka_task, writer_task],
            timeout=health_interval,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not done:
            print(
                f"[SUPERVISOR] Heartbeat: "
                f"kafka_consumer={'running' if not kafka_task.done() else 'stopped'}, "
                f"influx_writer={'running' if not writer_task.done() else 'stopped'}",
                flush=True,
            )
            continue

        if kafka_task in done:
            try:
                kafka_task.result()
                print(
                    "[SUPERVISOR] Kafka consumer exited unexpectedly.",
                    flush=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print(
                    f"[SUPERVISOR] Kafka consumer stopped: {error}",
                    flush=True,
                )

            await asyncio.sleep(restart_delay)

            kafka_task = asyncio.create_task(
                kafka_consumer(
                    memory_buffer, ack_queue, commit_state, config
                ),
                name="kafka_consumer",
            )
            print(
                "[SUPERVISOR] Kafka consumer restarted.",
                flush=True,
            )

        if writer_task in done:
            try:
                writer_task.result()
                print(
                    "[SUPERVISOR] InfluxDB writer exited unexpectedly.",
                    flush=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print(
                    f"[SUPERVISOR] InfluxDB writer stopped: {error}",
                    flush=True,
                )

            await asyncio.sleep(restart_delay)

            writer_task = asyncio.create_task(
                influx_writer(memory_buffer, ack_queue, config),
                name="influx_writer",
            )
            print(
                "[SUPERVISOR] InfluxDB writer restarted.",
                flush=True,
            )


async def main(config):

    memory_buffer = MemoryBuffer(
        maxsize=config.get("buffer_size", 10000)
    )
    ack_queue = asyncio.Queue()
    commit_state = CommitState()

    await supervisor(
        memory_buffer, ack_queue, commit_state, config
    )


if __name__ == "__main__":
    print("Starting N-Writer...", flush=True)
    print("Kafka topic:", WRITER_CONFIG["kafka_topic"], flush=True)
    print("InfluxDB URL:", WRITER_CONFIG["url"], flush=True)
    asyncio.run(main(WRITER_CONFIG))
