
import os

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:29093",
    ),
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_username": os.getenv("KAFKA_USERNAME", "admin"),
    "sasl_password": os.getenv("KAFKA_PASSWORD", "admin_secret"),
    "topic": os.getenv("KAFKA_TOPIC", "writer-test-topic"),
    "message_count": int(os.getenv("MESSAGE_COUNT", "10")),
    "message_interval": float(
        os.getenv("MESSAGE_INTERVAL", "1")
    ),
}
