import asyncio

from influxdb_client import InfluxDBClient
from confluent_kafka import Consumer, KafkaException


def initialize_kafka(config):

    try:
        consumer = Consumer(
            {
                "bootstrap.servers": config["bootstrap_servers"],
                "group.id": config["group_id"],
                "auto.offset.reset": config.get(
                    "auto_offset_reset",
                    "latest",
                ),

                "enable.auto.commit": False,
            }
        )

        return consumer

    except Exception as error:
        raise RuntimeError(
            f"Kafka initialization failed: {error}"
        ) from error


def handle_kafka_error(error):

    if isinstance(error, KafkaException):
        print(f"Kafka error: {error}")
    else:
        print(f"Unexpected Kafka error: {error}")


def initialize_influx_client(config):

    try:
        client = InfluxDBClient(
            url=config["url"],
            token=config["token"],
            org=config["org"],
            timeout=config.get("timeout", 10000),
            connection_pool_maxsize=config.get(
                "connection_pool_maxsize",
                10,
            ),
        )

        return client

    except Exception as error:
        raise RuntimeError(
            f"InfluxDB client initialization failed: {error}"
        ) from error


def initialize_write_api(client):

    try:
        return client.write_api()

    except Exception as error:
        raise RuntimeError(
            f"InfluxDB Write API initialization failed: {error}"
        ) from error


async def write_to_influx(write_api, message, config):

    try:
        await write_api.write(
            bucket=config["bucket"],
            org=config["org"],
            record=message,
        )

        return True

    except Exception as error:
        print(f"InfluxDB write error: {error}")
        return False


async def commit_kafka_message(consumer, message_data):

    try:

        await asyncio.to_thread(
            consumer.commit,

            message=message_data["record"],

            asynchronous=False,
        )

    except Exception as error:

        raise RuntimeError(
            f"Kafka commit failed: {error}"
        ) from error


async def close_kafka_consumer(consumer):

    if consumer is None:
        return

    try:

        await asyncio.to_thread(
            consumer.close
        )

    except Exception as error:

        print(
            f"Kafka consumer close error: {error}"
        )


async def close_influx_resources(client, write_api):

    if write_api is not None:

        try:

            await asyncio.to_thread(
                write_api.close
            )

        except Exception as error:

            print(
                f"InfluxDB Write API close error: "
                f"{error}"
            )

    if client is not None:

        try:

            await asyncio.to_thread(
                client.close
            )

        except Exception as error:

            print(
                f"InfluxDB client close error: "
                f"{error}"
            )