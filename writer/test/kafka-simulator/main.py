
import time

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
    

    base_timestamp_ns = 1793000000000000000
    timestamp_ns = base_timestamp_ns + sequence

    return (
        f"writer_test,source=simulator "
        f"message_id={sequence}i,value={sequence}i "
        f"{timestamp_ns}"
    )

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

            producer.produce(
                topic=topic,
                key=str(sequence),
                value=message.encode("utf-8"),
                callback=delivery_callback,
            )

            
            producer.poll(0)

            print(f"Queued message {sequence}: {message}")
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
