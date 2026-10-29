
import asyncio

from confluent_kafka import KafkaException
from confluent_kafka.aio import AIOConsumer

from confluent_kafka import TopicPartition
from influxdb_client.client.influxdb_client_async import (
    InfluxDBClientAsync,
)

class MemoryBuffer:
    def __init__(self, maxsize=10000):
        self.queue = asyncio.Queue(maxsize=maxsize)

    async def put(self, message):
        await self.queue.put(message)

    async def get(self):
        return await self.queue.get()

    def task_done(self):
        self.queue.task_done()


def initialize_kafka(config):
    try:
        return AIOConsumer({
            "bootstrap.servers": config["bootstrap_servers"],
            "group.id": config["group_id"],
            "auto.offset.reset": config.get(
                "auto_offset_reset", "earliest"
            ),
            "enable.auto.commit": False,
            "security.protocol": config.get(
                "security_protocol", "SASL_PLAINTEXT"
            ),
            "sasl.mechanism": config.get("sasl_mechanism", "PLAIN"),
            "sasl.username": config["sasl_username"],
            "sasl.password": config["sasl_password"],
        })
    except Exception as error:
        raise RuntimeError(
            f"Kafka initialization failed: {error}"
        ) from error


def handle_kafka_error(error):
    if isinstance(error, KafkaException):
        print(f"[KAFKA] Error: {error}", flush=True)
    else:
        print(f"[KAFKA] Unexpected error: {error}", flush=True)


def initialize_influx_client(config):
    try:
        return InfluxDBClientAsync(
            url=config["url"],
            token=config["token"],
            org=config["org"],
            timeout=config.get("timeout", 10000),
            connection_pool_maxsize=config.get(
                "connection_pool_maxsize", 10
            ),
        )
    except Exception as error:
        raise RuntimeError(
            f"InfluxDB client initialization failed: {error}"
        ) from error


def initialize_write_api(client, config):
    
    return client.write_api()


async def write_to_influx(write_api, message, config):
    try:
        await write_api.write(
            bucket=config["bucket"],
            org=config["org"],
            record=message,
        )
        return True
    except Exception as error:
        print(f"[INFLUX] Write error: {error}", flush=True)
        return False



async def commit_kafka_message(consumer, message_data):
    try:
        next_offset = message_data["offset"] + 1

        result = await consumer.commit(
            offsets=[
                TopicPartition(
                    message_data["topic"],
                    message_data["partition"],
                    next_offset,
                )
            ],
            asynchronous=False,
        )

        if result is None:
            raise RuntimeError("Kafka returned no commit result")

        for partition in result:
            if partition.error:
                raise RuntimeError(
                    f"Commit failed for {partition.topic}, "
                    f"partition {partition.partition}: "
                    f"{partition.error}"
                )

        print(
            f"[KAFKA] Committed next_offset={next_offset} "
            f"topic={message_data['topic']} "
            f"partition={message_data['partition']}",
            flush=True,
        )

    except Exception as error:
        raise RuntimeError(
            f"Kafka commit failed: {error}"
        ) from error


async def close_kafka_consumer(consumer):
    if consumer is None:
        return

    try:
        await consumer.close()
    except Exception as error:
        print(
            f"[KAFKA] Consumer close error: {error}",
            flush=True,
        )


async def close_influx_resources(client, write_api):
    if client is not None:
        try:
            await client.close()
        except Exception as error:
            print(
                f"[INFLUX] Write API close error: {error}",
                flush=True,
            )
