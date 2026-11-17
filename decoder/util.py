import asyncio
import json
import base64
import logging
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToDict
from aiokafka.structs import TopicPartition, OffsetAndMetadata
from protobuf_py import Perf3gppFields_pb2
from protobuf_py import VesEvent_pb2
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
KAFKA_BOOTSTRAP_SERVERS = "10.1.3.85:29093"
KAFKA_SECURITY_CONFIG = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN", 
    "sasl_plain_username": "admin",
    "sasl_plain_password": "admin_secret",
}
INPUT_TOPIC = "get_kafka_topic"
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
def validate_ves_event(ves_event):

    header = ves_event.commonEventHeader

    if (
        header.version != "1.0"
        or header.domain != "perf3gpp"
        or header.vesEventListenerVersion != "7.2"
    ):
        return False

    return True

def decode_ves_event(payload, source_name, kpi_names):
    try:
        ves_event = VesEvent_pb2.VesEvent()

        ves_event.ParseFromString(payload)

        if not validate_ves_event(ves_event):
            raise ValueError("Invalid VES event header")

        header = ves_event.commonEventHeader

        return decode_perf_event(
            ves_event.eventFields,
            header.reportingEntityName,
            source_name,
            kpi_names
        )
    except Exception as error:
        raise RuntimeError(
            f"VES Decode FAILED: {error}"
        ) from error
def validate_perf3gpp(perf):

    return perf.perf3gppFieldsVersion == "1.0"
def decode_perf_event(payload,reportingEntityName="unknown",sourceName="unknown",kpi_names=None):
    try:
        perf = Perf3gppFields_pb2.Perf3gppFields()
        perf.ParseFromString(payload)
        if not validate_perf3gpp(perf):
            raise ValueError(
                "Invalid PERF3GPP version: "
                f"{perf.perf3gppFieldsVersion}"
            )
        return decode_meas_data(
            perf,
            reportingEntityName,
            sourceName,
            kpi_names
        )
    except Exception as error:
        raise RuntimeError(
            f"PERF Decode FAILED: {error}"
        ) from error
def decode_meas_data(perf,reportingEntityName,sourceName,kpi_names=None):
    if perf is None:
        raise ValueError("PERF3GPP message is empty")
    metrics = []
    measurement = "hvves_data"
    for measInfo in perf.measDataCollection.measInfo:
        for measValue in measInfo.measValues:
            try:
                num_of_sectors = int(
                    measValue.measObjAddlFlds.get(
                        "num_of_sectors",
                        1
                    )
                )
            except (ValueError, TypeError):
                num_of_sectors = 1

            if num_of_sectors < 1:
                raise ValueError("Invalid number of sectors")

            meas_results = measValue.measResults

            result_count = len(meas_results)
            if kpi_names:
                names = list(kpi_names)
                values_per_sector = len(names)
            else:
                if result_count % num_of_sectors != 0:
                    raise ValueError(
                        "Cannot determine KPI positions per sector"
                    )
                values_per_sector = (
                    result_count // num_of_sectors
                )
                names = [
                    f"kpi_index_{idx}"
                    for idx in range(values_per_sector)
                ]
            for sector in range(num_of_sectors):
                sector_id = sector + 1
                offset = sector * values_per_sector
                for idx, kpi in enumerate(names):
                    index = offset + idx
                    if index >= result_count:
                        break
                    result = meas_results[index]
                    if result.HasField("iValue"):
                        value = result.iValue
                    elif result.HasField("rValue"):
                        value = result.rValue
                    else:
                        continue
                    metrics.append({
                        "measurement": measurement,
                        "tags": {
                            "kpiname": kpi,
                            "sourcename": sourceName,
                            "sector": sector_id
                        },
                        "fields": {
                            "kpivalue": value
                        }
                    })

    return metrics
def transform_to_line_protocol(metric):
    measurement = metric["measurement"]
    def escape_tag(value):
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace(",", "\\,")
            .replace(" ", "\\ ")
            .replace("=", "\\=")
        )
    def escape_measurement(value):
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace(",", "\\,")
            .replace(" ", "\\ ")
        )
    tags = ",".join(
        f"{escape_tag(k)}={escape_tag(v)}"
        for k, v in metric["tags"].items()
    )

def decode_perf(encoded_data):
    try:
        if not isinstance(encoded_data, bytes):
            raise TypeError(
                "Encoded data must be bytes"
            )
        if not encoded_data:
            raise ValueError(
                "Encoded data is empty"
            )
        decoded_data = decode_perf_event(
            payload=encoded_data,
            reportingEntityName="unknown",
            sourceName="unknown",
            kpi_names=None
        )
        return decoded_data
    except Exception as error:
        raise RuntimeError(
            f"PERF3GPP decode failed: {error}"
        ) from error
async def check_kafka_health():
    logger.info("Checking Kafka connection")
    retry_backoff = (INITIAL_RETRY_BACKOFF)
    while True:
        health_client = None
        try:
            health_client = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id="perf3gpp-health-check",
                request_timeout_ms=5000,
                **KAFKA_SECURITY_CONFIG,
            )
            await asyncio.wait_for(health_client.start(),timeout=5,)
            logger.info("Kafka is reachable")
            return
        except asyncio.CancelledError:
            logger.info("Kafka health check cancelled")
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
async def kafka_consumer_worker(process_pool,memory_buffer,kafka_failure_event,commit_queue,):
    logger.info("Kafka Consumer Worker started")
    consumer = None
    try:
        consumer = AIOKafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id=CONSUMER_ID,
            **KAFKA_SECURITY_CONFIG,
        )
        await consumer.start()
        logger.info(
            "Kafka consumer connected | consumer_id=%s | topic=%s",
            CONSUMER_ID,
            INPUT_TOPIC,
        )
        loop = asyncio.get_running_loop()
        while True:
            records = await consumer.getmany(
                timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                max_records=CONSUMER_MAX_POLL_RECORDS,
            )
            for topic_partition, messages in records.items():
                logger.info(
                    "Kafka batch received | topic=%s | partition_id=%s | message_count=%s",
                    topic_partition.topic,
                    topic_partition.partition,
                    len(messages),
                )
                for message in messages:
                    logger.info(
                        "Consuming Kafka message | topic=%s | partition_id=%s | offset=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    try:
                        decoded_data = await loop.run_in_executor(
                            process_pool,
                            decode_perf,
                            message.value,
                        )
                        await memory_buffer.put({
                            "decoded_data": decoded_data,
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                        })
                    except Exception as error:
                        logger.error(
                            "Message processing failed | topic=%s | partition_id=%s | offset=%s | error=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                            error,
                        )
                        continue
            while not commit_queue.empty():
                topic, partition, offset = await commit_queue.get()

                try:
                    await consumer.commit({
                        TopicPartition(topic, partition):
                            OffsetAndMetadata(offset + 1, "")
                    })

                    logger.info(
                        "Kafka offset committed | topic=%s | "
                        "partition=%s | committed_offset=%s",
                        topic,
                        partition,
                        offset + 1,
                    )

                finally:
                    commit_queue.task_done()

    except asyncio.CancelledError:
        raise

    except Exception as error:
        logger.exception("Consumer worker failed | error=%s", error)
        raise

    finally:
        if consumer is not None:
            try:
                await consumer.stop()
            except Exception as error:
                logger.warning("Error stopping consumer | error=%s", error)

        logger.info("Kafka Consumer Worker stopped")
async def kafka_producer_worker(memory_buffer,retry_queue,kafka_failure_event,commit_queue,):
    logger.info("Kafka Producer Worker started")
    producer = None
    current_item = None
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id=PRODUCER_ID,
            acks="all",
            enable_idempotence=True,
            linger_ms=PRODUCER_LINGER_MS,
            max_batch_size=PRODUCER_MAX_BATCH_SIZE,
            request_timeout_ms=30000,
            retry_backoff_ms=500,
            **KAFKA_SECURITY_CONFIG,
        )
        await producer.start()
        logger.info(
            "Producer connected | producer_id=%s | topic=%s",
            PRODUCER_ID,
            OUTPUT_TOPIC,
        )
        while True:
            current_item = await memory_buffer.get()
            try:
                message_bytes = json.dumps(current_item["decoded_data"]).encode("utf-8")
                metadata = await producer.send_and_wait(OUTPUT_TOPIC,message_bytes,)
                await commit_queue.put((current_item["topic"],current_item["partition"],current_item["offset"],))
                logger.info(
                    "Message produced | topic=%s | partition=%s | offset=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )
                memory_buffer.task_done()
                current_item = None
            except MessageSizeTooLargeError as error:
                logger.error(
                    "Message too large | partition=%s | offset=%s | error=%s",
                    current_item.get("partition"),
                    current_item.get("offset"),
                    error,
                )
                memory_buffer.task_done()
                current_item = None
            except KAFKA_CONNECTION_ERRORS as error:
                logger.error(
                    "Producer Kafka connection failure | error=%s",
                    error,
                )
                kafka_failure_event.set()
                await retry_queue.put({
                    "message": current_item,
                    "error": str(error),
                    "error_type": "kafka_connection",
                    "retry_count": 0,
                })
                memory_buffer.task_done()
                current_item = None
                raise
    except asyncio.CancelledError:
        logger.info("Kafka Producer Worker cancellation requested")

        if current_item is not None:
            await retry_queue.put({
                "message": current_item,
                "error": "Producer shutdown",
                "error_type": "shutdown",
                "retry_count": 0,
            })

            memory_buffer.task_done()
            current_item = None

        raise
    except Exception as error:
        logger.exception("Producer Worker failed | error=%s", error)
        raise

    finally:
        if producer is not None:
            try:
                await producer.flush()
            except Exception as error:
                logger.warning("Producer flush failed | %s", error)

            try:
                await producer.stop()
            except Exception as error:
                logger.warning("Producer stop failed | %s", error)

        logger.info("Kafka Producer Worker stopped")
async def kafka_retry_worker(retry_queue, commit_queue):
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
        **KAFKA_SECURITY_CONFIG,
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
                await commit_queue.put((
                    failed_message["topic"],
                    failed_message["partition"],
                    failed_message["offset"],
                ))
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
def start_workers(process_pool,memory_buffer,retry_queue,kafka_failure_event,commit_queue):

    logger.info(
        "Starting Kafka workers"
    )
    consumer_worker = asyncio.create_task(
        kafka_consumer_worker(
            process_pool,
            memory_buffer,
            kafka_failure_event,
            commit_queue,
        ),
        name="kafka-consumer-worker",
    )
    producer_worker = asyncio.create_task(
        kafka_producer_worker(
            memory_buffer,
            retry_queue,
            kafka_failure_event,
            commit_queue,
        ),
        name="kafka-producer-worker",
    )
    retry_worker = asyncio.create_task(
    kafka_retry_worker(
        retry_queue,
        commit_queue,
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
async def supervisor(process_pool,memory_buffer,retry_queue,kafka_failure_event,commit_queue,):
    logger.info("Supervisor started")
    workers = {}
    restart_delay = 0.01
    try:
        await check_kafka_health()
        workers = start_workers(
            process_pool,
            memory_buffer,
            retry_queue,
            kafka_failure_event,
            commit_queue,
        )
        while True:
            done, _ = await asyncio.wait(
                workers,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for finished_task in done:
                worker_name = finished_task.get_name()
                if workers.get(worker_name) is not finished_task:
                    continue
                if finished_task.cancelled():
                    logger.warning("Worker was cancelled unexpectedly | worker=%s",worker_name,)
                else:
                    error = finished_task.exception()
                    if error is None:
                        logger.warning(
                            "Worker exited unexpectedly | worker=%s",
                            worker_name,
                        )
                    else:
                        logger.error(
                            "Worker crashed | worker=%s | error=%s",
                            worker_name,
                            error,
                            exc_info=(
                                type(error),
                                error,
                                error.__traceback__,
                            ),
                        )
                workers.pop(worker_name)
                if worker_name == "kafka-producer-worker":
                    kafka_failure_event.clear()
                await asyncio.sleep(restart_delay)
                if worker_name == "kafka-consumer-worker":
                    replacement_task = asyncio.create_task(
                        kafka_consumer_worker(
                            process_pool,
                            memory_buffer,
                            kafka_failure_event,
                            commit_queue,
                        ),
                        name="kafka-consumer-worker",
                    )
                elif worker_name == "kafka-producer-worker":
                    replacement_task = asyncio.create_task(
                        kafka_producer_worker(
                            memory_buffer,
                            retry_queue,
                            kafka_failure_event,
                            commit_queue,
                        ),
                        name="kafka-producer-worker",
                    )
                elif worker_name == "kafka-retry-worker":
                    replacement_task = asyncio.create_task(
                        kafka_retry_worker(
                            retry_queue,
                            commit_queue,
                        ),
                        name="kafka-retry-worker",
                    )
                else:
                    logger.error(
                        "Unknown worker cannot be restarted | worker=%s",
                        worker_name,
                    )
                    continue
                workers[worker_name] = replacement_task
                logger.info(
                    "Worker restarted | worker=%s | restart_delay_ms=%s",
                    worker_name,
                    restart_delay * 1000,
                )
    except asyncio.CancelledError:
        logger.info("Supervisor cancelled")
        raise
    finally:
        await stop_workers(workers)
        logger.info("Supervisor stopped")