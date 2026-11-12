
import time
import json

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
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered: topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}"
    )



def create_message(sequence):
    return {
        "sourcename": "client5010_f00d0c4b",
        "streamId": "B48256",
        "metrics": [
            {"kpiname": "RRU.PrbAvailDl", "sector": 1, "kpivalue": 6.0},
            {"kpiname": "RRU.PrbAvailUl", "sector": 1, "kpivalue": 9.0},
            {"kpiname": "RRU.PrbUsedDl", "sector": 1, "kpivalue": 0.0},
            {"kpiname": "RRU.PrbUsedUl", "sector": 1, "kpivalue": 8.0},
            {"kpiname": "RRU.PrbAvailDl", "sector": 2, "kpivalue": 8.0},
            {"kpiname": "RRU.PrbAvailUl", "sector": 2, "kpivalue": 3.0},
            {"kpiname": "RRU.PrbUsedDl", "sector": 2, "kpivalue": 8.0},
            {"kpiname": "RRU.PrbUsedUl", "sector": 2, "kpivalue": 4.0}
        ]
    }


def produce_messages():
    producer = create_producer()
    topic = KAFKA_CONFIG["topic"]
    count = KAFKA_CONFIG["message_count"]
    interval = KAFKA_CONFIG["message_interval"]

    print(f"Broker: {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"Topic: {topic}")
    print(f"Sending {count} test messages...")

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

            print(f"Queued message {sequence}: {message_json}")
            time.sleep(interval)

    except BufferError as error:
        print(f"Producer queue is full: {error}")

    finally:
        remaining = producer.flush(timeout=30)

        if remaining:
            print(f"Warning: {remaining} message(s) remain undelivered.")
        else:
            print("All queued messages have completed delivery.")

    print("Simulator finished.")


if __name__ == "__main__":
    produce_messages()
