import asyncio
import json
import base64
import logging

from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
)

from aiokafka.errors import (
    KafkaConnectionError,
    KafkaTimeoutError,
    KafkaUnavailableError,
    NoBrokersAvailable,
    MessageSizeTooLargeError,
    KafkaError,
)
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "PERF3GPP_TOPIC"
OUTPUT_TOPIC = "DECODED_PERF"

CONSUMER_GROUP_ID = "perf3gpp-decoder-group"

CONSUMER_ID = "perf3gpp-consumer"
PRODUCER_ID = "perf3gpp-producer"

SUPERVISOR_MONITOR_INTERVAL = 1

INITIAL_RETRY_BACKOFF = 1
MAX_RETRY_BACKOFF = 30

PROCESS_POOL_WORKERS = 4
BUFFER_MAX_SIZE = 10000

CONSUMER_POLL_TIMEOUT_MS = 1000
CONSUMER_MAX_POLL_RECORDS = 5000

FAILED_DECODE_FILE = "failed_decode_messages.jsonl"
FAILED_DECODE_BATCH_SIZE = 100
FAILED_DECODE_FLUSH_INTERVAL = 1.0

PRODUCER_LINGER_MS = 40
PRODUCER_MAX_BATCH_SIZE = 65536

PRODUCER_RETRY_MAX_ATTEMPTS = 4
PRODUCER_RETRY_DELAY = 1


logger = logging.getLogger("perf3gpp")
KAFKA_CONNECTION_ERRORS = (
    KafkaConnectionError,
    KafkaUnavailableError,
    NoBrokersAvailable,
    KafkaTimeoutError,
)
def decode_perf(encoded_data):

    try:

        decoded_data = {
            "status": "decoded",
            "size": len(encoded_data),
        }

        return decoded_data

    except Exception as error:

        raise RuntimeError(
            f"PERF3GPP decode failed: {error}"
        )
def prepare_failed_decode_message(
    failed_message,
):

    record = {

        "consumer_id":
            failed_message["consumer_id"],

        "topic":
            failed_message["topic"],

        "partition":
            failed_message["partition"],

        "offset":
            failed_message["offset"],

        "encoded_data":
            base64.b64encode(
                failed_message["encoded_data"]
            ).decode("utf-8"),

        "error":
            failed_message["error"],
    }

    return json.dumps(record) + "\n"

def store_failed_decode_batch(
    records,
):

    with open(
        FAILED_DECODE_FILE,
        "a",
        encoding="utf-8",
    ) as file:

        file.writelines(records)
async def flush_failed_decode_batch(
    batch,
    failed_decode_queue,
):

    if not batch:
        return

    records = [

        prepare_failed_decode_message(
            failed_message
        )

        for failed_message in batch
    ]

    await asyncio.to_thread(
        store_failed_decode_batch,
        records,
    )

    for _ in batch:

        failed_decode_queue.task_done()

    logger.info(
        "Failed decode batch written | "
        "count=%s | file=%s",
        len(batch),
        FAILED_DECODE_FILE,
    )

async def failed_decode_writer(failed_decode_queue,):
    logger.info(
        "Failed Decode Writer started"
    )
    batch = []
    loop = asyncio.get_running_loop()
    last_flush_time = loop.time()
    try:
        while True:
            elapsed_time = (
                loop.time()
                - last_flush_time
            )

            remaining_time = max(
                0,
                FAILED_DECODE_FLUSH_INTERVAL
                - elapsed_time,
            )

            try:

                failed_message = (
                    await asyncio.wait_for(
                        failed_decode_queue.get(),
                        timeout=remaining_time,
                    )
                )

                batch.append(
                    failed_message
                )

            except asyncio.TimeoutError:

                pass


            batch_full = (
                len(batch)
                >= FAILED_DECODE_BATCH_SIZE
            )


            flush_timeout = (
                batch
                and (
                    loop.time()
                    - last_flush_time
                    >= FAILED_DECODE_FLUSH_INTERVAL
                )
            )


            if (
                batch_full
                or flush_timeout
            ):

                try:

                    await flush_failed_decode_batch(
                        batch,
                        failed_decode_queue,
                    )
                    batch = []
                    last_flush_time = (loop.time())
                except Exception as error:
                    logger.error(
                        "Failed decode write failed | "
                        "error=%s",
                        error,
                    )
                    await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info(
            "Failed Decode Writer "
            "cancellation requested"
        )
        if batch:
            await flush_failed_decode_batch(
                batch,
                failed_decode_queue,
            )

        logger.info(
            "Failed Decode Writer stopped"
        )

        raise
async def check_kafka_health():

    logger.info(
        "Checking Kafka connection"
    )

    retry_backoff = (
        INITIAL_RETRY_BACKOFF
    )

    while True:

        health_client = None

        try:

            health_client = AIOKafkaProducer(
                bootstrap_servers=(
                    KAFKA_BOOTSTRAP_SERVERS
                ),
                client_id=(
                    "perf3gpp-health-check"
                ),
                request_timeout_ms=5000,
            )

            await asyncio.wait_for(
                health_client.start(),
                timeout=5,
            )

            logger.info(
                "Kafka is reachable"
            )

            return


        except asyncio.CancelledError:

            logger.info(
                "Kafka health check cancelled"
            )

            raise


        except Exception as error:

            logger.warning(
                "Kafka is not reachable | "
                "error=%s | "
                "retry_after=%s seconds",
                error,
                retry_backoff,
            )


        finally:

            if health_client is not None:

                try:

                    await health_client.stop()

                except Exception:

                    pass


        await asyncio.sleep(
            retry_backoff
        )


        retry_backoff = min(
            retry_backoff * 2,
            MAX_RETRY_BACKOFF,
        )
async def kafka_consumer_worker(
    process_pool,
    memory_buffer,
    failed_decode_queue,
    kafka_failure_event,
):
    logger.info(
        "Kafka Consumer Worker started"
    )

    consumer = None

    try:

        consumer = AIOKafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=(
                KAFKA_BOOTSTRAP_SERVERS
            ),
            group_id=CONSUMER_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id=CONSUMER_ID,
        )
        await consumer.start()
        logger.info(
            "Consumer connected | "
            "consumer_id=%s | "
            "topic=%s",
            CONSUMER_ID,
            INPUT_TOPIC,
        )
        loop = asyncio.get_running_loop()
        while True:
            records = await consumer.getmany(
                timeout_ms=(
                    CONSUMER_POLL_TIMEOUT_MS
                ),
                max_records=(
                    CONSUMER_MAX_POLL_RECORDS
                ),
            )
            for (topic_partition, messages,) in records.items():
                logger.info(
                    "Messages polled | "
                    "topic=%s | "
                    "partition=%s | "
                    "count=%s",
                    topic_partition.topic,
                    topic_partition.partition,
                    len(messages),
                )
                for message in messages:
                    encoded_data = (
                        message.value
                    )
                    try:
                        decoded_data = (
                            await loop.run_in_executor(
                                process_pool,
                                decode_perf,
                                encoded_data,
                            )
                        )
                        decoded_message = {
                            "consumer_id":CONSUMER_ID,
                            "source_topic":message.topic,
                            "partition":message.partition,
                            "offset":message.offset,
                            "decoded_data":decoded_data,
                        }
                        await memory_buffer.put(decoded_message)
                        logger.info(
                            "Decode successful | "
                            "partition=%s | "
                            "offset=%s",
                            message.partition,
                            message.offset,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        failed_message = {
                            "consumer_id":CONSUMER_ID,
                            "topic":message.topic,
                            "partition":message.partition,
                            "offset":message.offset,
                            "encoded_data":message.value,
                            "error":str(error),
                        }
                        await failed_decode_queue.put(failed_message)
                        logger.error(
                            "Decode failed | "
                            "partition=%s | "
                            "offset=%s | "
                            "error=%s",
                            message.partition,
                            message.offset,
                            error,
                        )
    except asyncio.CancelledError:

        logger.info(
            "Kafka Consumer Worker "
            "cancellation requested"
        )

        raise


    except KAFKA_CONNECTION_ERRORS as error:

        logger.error(
            "Consumer Kafka connection "
            "failure | error=%s",
            error,
        )

        kafka_failure_event.set()

        raise


    except Exception as error:

        logger.exception(
            "Consumer Worker crashed | "
            "error=%s",
            error,
        )

        raise


    finally:

        if consumer is not None:

            try:

                await consumer.stop()

            except Exception as error:

                logger.warning(
                    "Error stopping Consumer | %s",
                    error,
                )

        logger.info(
            "Kafka Consumer Worker stopped"
        )


async def kafka_producer_worker(memory_buffer,retry_queue,kafka_failure_event,):
    logger.info("Kafka Producer Worker started")
    producer = None
    current_item = None
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=(
                KAFKA_BOOTSTRAP_SERVERS
            ),
            client_id=PRODUCER_ID,
            acks="all",
            enable_idempotence=True,
            linger_ms=PRODUCER_LINGER_MS,
            max_batch_size=(
                PRODUCER_MAX_BATCH_SIZE
            ),
            request_timeout_ms=30000,
            retry_backoff_ms=500,
        )
        await producer.start()
        logger.info(
            "Producer connected | "
            "producer_id=%s | "
            "topic=%s",
            PRODUCER_ID,
            OUTPUT_TOPIC,
        )
        while True:
            current_item = (
                await memory_buffer.get()
            )
            try:
                message_bytes = json.dumps(
                    current_item[
                        "decoded_data"
                    ]
                ).encode("utf-8")
                metadata = (
                    await producer.send_and_wait(
                        OUTPUT_TOPIC,
                        message_bytes,
                    )
                )
                logger.info(
                    "Message produced | "
                    "topic=%s | "
                    "partition=%s | "
                    "offset=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )
                memory_buffer.task_done()

                current_item = None
            except KAFKA_CONNECTION_ERRORS as error:

                logger.error(
                    "Producer Kafka connection "
                    "failure | error=%s",
                    error,
                )
                kafka_failure_event.set()
                await retry_queue.put(
                    {
                        "message":
                            current_item,

                        "error":
                            str(error),

                        "error_type":
                            "kafka_connection",

                        "retry_count":
                            0,
                    }
                )
                memory_buffer.task_done()

                current_item = None

                raise

            except MessageSizeTooLargeError as error:
                logger.error(
                    "Message too large | "
                    "partition=%s | "
                    "offset=%s | "
                    "error=%s",
                    current_item.get(
                        "partition"
                    ),
                    current_item.get(
                        "offset"
                    ),
                    error,
                )

                memory_buffer.task_done()

                current_item = None


            except KafkaError as error:

                logger.error(
                    "Kafka Producer error | "
                    "error=%s",
                    error,
                )


                await retry_queue.put(
                    {
                        "message":
                            current_item,

                        "error":
                            str(error),

                        "error_type":
                            "kafka_error",

                        "retry_count":
                            0,
                    }
                )


                memory_buffer.task_done()

                current_item = None


            except Exception as error:

                logger.exception(
                    "Unexpected Producer error | "
                    "error=%s",
                    error,
                )
                await retry_queue.put(
                    {
                        "message":
                            current_item,

                        "error":
                            str(error),

                        "error_type":
                            "unexpected_error",

                        "retry_count":
                            0,
                    }
                )
                memory_buffer.task_done()
                current_item = None
    except asyncio.CancelledError:
        logger.info(
            "Kafka Producer Worker "
            "cancellation requested"
        )
        if current_item is not None:

            await retry_queue.put(
                {
                    "message":
                        current_item,

                    "error":
                        "Producer shutdown",

                    "error_type":
                        "shutdown",

                    "retry_count":
                        0,
                }
            )
            memory_buffer.task_done()
            current_item = None
        raise
    except KAFKA_CONNECTION_ERRORS as error:
        kafka_failure_event.set()
        logger.error(
            "Producer Kafka connection "
            "failed | error=%s",
            error,
        )

        raise


    except Exception as error:
        logger.exception(
            "Producer Worker crashed | "
            "error=%s",
            error,
        )

        raise


    finally:

        if producer is not None:

            try:

                await producer.flush()

            except Exception as error:

                logger.warning(
                    "Producer flush failed | %s",
                    error,
                )


            try:

                await producer.stop()

            except Exception as error:

                logger.warning(
                    "Producer stop failed | %s",
                    error,
                )

        logger.info(
            "Kafka Producer Worker stopped"
        )
async def kafka_retry_worker(retry_queue):
    logger.info("Kafka Retry Worker started")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="perf3gpp-retry-producer",
        acks="all",
        enable_idempotence=True,
        linger_ms=PRODUCER_LINGER_MS,
        max_batch_size=PRODUCER_MAX_BATCH_SIZE,
        request_timeout_ms=30000,
        retry_backoff_ms=500,
    )

    await producer.start()

    RETRY_BATCH_SIZE = 100
    RETRY_BATCH_FLUSH_INTERVAL = 1.0

    try:
        while True:

            batch = []
            first_item = await retry_queue.get()
            batch.append(first_item)

            start_time = asyncio.get_running_loop().time()

            while len(batch) < RETRY_BATCH_SIZE:

                elapsed_time = (
                    asyncio.get_running_loop().time()
                    - start_time
                )

                remaining_time = (
                    RETRY_BATCH_FLUSH_INTERVAL
                    - elapsed_time
                )

                if remaining_time <= 0:
                    break

                try:
                    item = await asyncio.wait_for(
                        retry_queue.get(),
                        timeout=remaining_time,
                    )

                    batch.append(item)

                except asyncio.TimeoutError:
                    break

            logger.info(
                "Retry batch collected | size=%s",
                len(batch),
            )
            for retry_item in batch:

                failed_message = retry_item["message"]

                payload = json.dumps(
                    failed_message["decoded_data"]
                ).encode("utf-8")

                metadata = await producer.send_and_wait(
                    OUTPUT_TOPIC,
                    payload,
                )

                logger.info(
                    "Retry message produced | "
                    "topic=%s | "
                    "partition=%s | "
                    "offset=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )
                retry_queue.task_done()
            batch.clear()
    finally:
        await producer.flush()
        await producer.stop()

        logger.info(
            "Kafka Retry Worker stopped"
        )
def start_workers(
    process_pool,
    memory_buffer,
    retry_queue,
    failed_decode_queue,
    kafka_failure_event,
):

    logger.info(
        "Starting Kafka workers"
    )
    consumer_worker = asyncio.create_task(
        kafka_consumer_worker(
            process_pool,
            memory_buffer,
            failed_decode_queue,
            kafka_failure_event,
        ),
        name="kafka-consumer-worker",
    )


    producer_worker = asyncio.create_task(
        kafka_producer_worker(
            memory_buffer,
            retry_queue,
            kafka_failure_event,
        ),
        name="kafka-producer-worker",
    )
    retry_worker = asyncio.create_task(
        kafka_retry_worker(
            retry_queue,
        ),
        name="kafka-retry-worker",
    )
    return [
        consumer_worker,
        producer_worker,
        retry_worker,
    ]
async def stop_worker(
    worker,
    worker_name,
):

    if worker is None:
        return

    if worker.done():

        try:
            worker.result()
        except (
            asyncio.CancelledError,
            Exception,
        ):
            pass

        return
    logger.info(
        "Stopping %s",
        worker_name,
    )
    worker.cancel()
    try:

        await worker

    except asyncio.CancelledError:

        pass

    except Exception as error:

        logger.warning(
            "%s stopped with error | %s",
            worker_name,
            error,
        )
async def stop_workers(
    workers,
):

    if not workers:
        return


    logger.info(
        "Stopping Kafka workers"
    )
    for worker in workers:

        if not worker.done():
            worker.cancel()


    await asyncio.gather(
        *workers,
        return_exceptions=True,
    )


    logger.info(
        "Kafka workers stopped"
    )
async def wait_for_worker_failure(workers,kafka_failure_event,):

    event_task = asyncio.create_task(
        kafka_failure_event.wait()
    )

    try:

        done, _ = await asyncio.wait(
            [
                *workers,
                event_task,
            ],
            return_when=(
                asyncio.FIRST_COMPLETED
            ),
        )
        if event_task in done:

            return "kafka_failure"
        return "worker_failure"


    finally:

        if not event_task.done():

            event_task.cancel()

            try:
                await event_task
            except asyncio.CancelledError:
                pass

async def supervisor(
    process_pool,
    memory_buffer,
    retry_queue,
    failed_decode_queue,
    kafka_failure_event,
):

    logger.info("Supervisor started")

    workers = []

    try:

        while True:

            await check_kafka_health()
            workers = start_workers(
                process_pool,
                memory_buffer,
                retry_queue,
                failed_decode_queue,
                kafka_failure_event,
            )
            reason = await wait_for_worker_failure(
                workers,
                kafka_failure_event,
            )

            logger.warning(
                "Restarting workers | reason=%s",
                reason,
            )

            await stop_workers(workers)

            workers = []

    except asyncio.CancelledError:

        logger.info(
            "Supervisor cancelled"
        )

        raise

    finally:

        await stop_workers(workers)

        logger.info(
            "Supervisor stopped"
        )