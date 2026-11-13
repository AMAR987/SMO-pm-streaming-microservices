import time
import json
import random
import uuid

from confluent_kafka import Producer

from config import KAFKA_CONFIG


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
        "security.protocol": KAFKA_CONFIG["security_protocol"],
        "sasl.mechanism": KAFKA_CONFIG["sasl_mechanism"],
        "sasl.username": KAFKA_CONFIG["sasl_username"],
        "sasl.password": KAFKA_CONFIG["sasl_password"],
    })


def delivery_callback(error, message):
    if error:
        print(f"[PRODUCER] Delivery failed: {error}", flush=True)
        return

    print(
        f"[PRODUCER] Delivered: topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}",
        flush=True,
    )


def create_message(sequence):
    metrics = []

    kpi_names = [
        "RRC.ConnMean",
        "RRC.ConnMax",
        "RRC.InactiveConnMean",
        "RRC.InactiveConnMax",
    ]

    for sector in [1, 2]:
        for kpiname in kpi_names:
            metrics.append({
                "kpiname": kpiname,
                "sector": sector,
                "kpivalue": round(random.uniform(0, 10), 2),
            })

    return {
        "sourcename": f"client_{uuid.uuid4().hex[:8]}",
        "streamId": f"stream_{sequence}",
        "metrics": metrics,
    }


def produce_messages():
    producer = create_producer()

    topic = KAFKA_CONFIG["topic"]
    count = KAFKA_CONFIG["message_count"]
    interval = KAFKA_CONFIG["message_interval"]

    print(
        f"[PRODUCER] Broker: {KAFKA_CONFIG['bootstrap_servers']}",
        flush=True,
    )
    print(f"[PRODUCER] Topic: {topic}", flush=True)
    print(f"[PRODUCER] Sending {count} test messages...", flush=True)

    try:
        for sequence in range(1, count + 1):
            message = create_message(sequence)
            message_json = json.dumps(message)

            producer.produce(
                topic=topic,
                key=str(sequence),
                value=message_json.encode("utf-8"),
                callback=delivery_callback,
            )

            producer.poll(0)

            print(
                f"[PRODUCER] Queued message {sequence}: {message_json}",
                flush=True,
            )

            time.sleep(interval)

    except BufferError as error:
        print(f"[PRODUCER] Queue is full: {error}", flush=True)

    finally:
        remaining = producer.flush(timeout=30)

        if remaining:
            print(
                f"[PRODUCER] Warning: {remaining} messages undelivered.",
                flush=True,
            )
        else:
            print("[PRODUCER] All messages delivered.", flush=True)

    print("[PRODUCER] Simulator finished.", flush=True)


if __name__ == "__main__":
    produce_messages()
