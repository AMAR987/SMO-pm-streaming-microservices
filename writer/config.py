import os

WRITER_CONFIG = {
    
    "bootstrap_servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29093"
    ),
    "group_id": os.getenv(
        "KAFKA_GROUP_ID", "n-writer-test-group"
    ),
    "kafka_topic": os.getenv(
        "KAFKA_TOPIC", "writer-test-topic"
    ),
    "auto_offset_reset": "earliest",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_username": os.getenv("KAFKA_USERNAME", "admin"),
    "sasl_password": os.getenv(
        "KAFKA_PASSWORD", "admin_secret"
    ),


    "url": os.getenv("INFLUX_URL", "http://localhost:8086"),
    "token": os.getenv("INFLUX_TOKEN", "RUVu5qHGRw9EuTl61-7eIhryXa6A31DzH0y7NGii9HrIBdLbv29h3pjzj14z6gPlT_u9qNFGI1ZtioIEpaqgPw=="),
    "org": os.getenv("INFLUX_ORG", "org"),
    "bucket": os.getenv("INFLUX_BUCKET", "bucket"),


    "buffer_size": 10000,
    "poll_timeout": 1.0,
    "retry_delay": 5,
    "restart_delay": 5,
    "batch_size": 1000,
    "flush_interval": 1000,
    "retry_interval": 5000,
    "max_retries": 5,
    "timeout": 10000,
    "connection_pool_maxsize": 10,
}