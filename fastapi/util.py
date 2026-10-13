import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator
from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

logger = logging.getLogger("hv-ves")

class StreamInfoRequest(BaseModel):
    """One stream entry in an O1 HV-VES request."""
    model_config = ConfigDict(populate_by_name=True, extra="ignore")
    stream_id: str = Field(..., alias="streamId", min_length=1)
    stream_type: str = Field(..., alias="StreamType", min_length=1)
    serialization_format: str = Field("GPB", alias="SerializationFormat")
    meas_obj_dn: str = Field("", alias="measObjDn")
    performance_metrics: List[Any] = Field(default_factory=list, alias="performanceMetrics")
    vs_data_container: Dict[str, Any] = Field(default_factory=dict, alias="VsDataContainer")

    @model_validator(mode="before")
    @classmethod
    def unwrap_legacy_stream_wrapper(cls, values):
        # Stream-add has appeared as a direct stream and as a legacy wrapper.
        if isinstance(values, dict) and "streamInfoList" in values:
            wrapped = values["streamInfoList"]
            if isinstance(wrapped, list):
                if len(wrapped) != 1:
                    raise ValueError("streamInfoList must contain exactly one stream for this endpoint")
                wrapped = wrapped[0]
            if not isinstance(wrapped, dict):
                raise ValueError("streamInfoList must be a stream object")
            values = wrapped
        return values

    @model_validator(mode="after")
    def validate_stream_details(self):
        stream_type = self.stream_type.strip().upper()
        if stream_type in {"PERFORMANCE", "PERF3GPP"} and not self.performance_metrics:
            raise ValueError("performanceMetrics must contain at least one item for performance streams")
        if stream_type not in {"PERFORMANCE", "PERF3GPP", "PROPRIETARY"}:
            raise ValueError("StreamType must be PERFORMANCE, PERF3GPP, or PROPRIETARY")
        if self.serialization_format.strip().upper() != "GPB":
            raise ValueError("Only GPB SerializationFormat is supported")
        return self

class ConnectionRequest(BaseModel):
    """Create-connection request; supports legacy object and list streamInfoList."""
    model_config = ConfigDict(populate_by_name=True, extra="ignore")
    producer_id: str = Field(..., alias="producerId", min_length=1)
    stream_info_list: List[StreamInfoRequest] = Field(..., alias="streamInfoList", min_length=1)

    @model_validator(mode="before")
    @classmethod
    def normalize_stream_info_list(cls, values):
        if not isinstance(values, dict):
            return values
        stream_info = values.get("streamInfoList", values.get("stream_info_list"))
        if isinstance(stream_info, dict):
            values = dict(values)
            values["streamInfoList"] = [stream_info]
        return values

class StreamRequest(StreamInfoRequest):
    """Request body used to add one stream to an existing connection."""
CONNECTION_TTL = int(os.getenv("CONNECTION_TTL", "300"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_SYNC_INTERVAL = float(os.getenv("REDIS_SYNC_INTERVAL", "2"))
REDIS_RETRY_COUNT = int(os.getenv("REDIS_RETRY_COUNT", "5"))
REDIS_BACKOFF_BASE = float(os.getenv("REDIS_BACKOFF_BASE", "1"))
REDIS_BACKOFF_CAP = float(os.getenv("REDIS_BACKOFF_CAP", "10"))
REDIS_HEALTH_CHECK_INTERVAL = int(
    os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "1")
)
REDIS_SOCKET_CONNECT_TIMEOUT = float(
    os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "2")
)
REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
REDIS_RETRY = Retry(
    ExponentialBackoff(
        cap=REDIS_BACKOFF_CAP,
        base=REDIS_BACKOFF_BASE,
    ),
    REDIS_RETRY_COUNT,
)
REDIS_RETRY_ERRORS = [
    RedisConnectionError,
    RedisTimeoutError,
    ConnectionResetError,
]

@dataclass
class StreamInfo:
    stream_id: str
    stream_type: str
    serialization_format: str = "GPB"
    meas_obj_dn: str = ""
    performance_metrics: List[Any] = field(default_factory=list)
    vs_data_container: Dict[str, Any] = field(default_factory=dict)
    last_activity: float = field(default_factory=time.time)


@dataclass
class ConnectionInfo:
    connection_id: str
    producer_id: str = ""
    default_stream_id: Optional[str] = None
    streams: Dict[str, StreamInfo] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)

class ClientInfo:

    def __init__(self):
        self.redis: Optional[Redis] = None
        self.redis_sync_task: Optional[asyncio.Task] = None
        self.redis_init_lock = asyncio.Lock()
        self.pending_upserts: Dict[str, ConnectionInfo] = {}
        self.pending_deletes: set[str] = set()

    async def init_redis(self) -> bool:
        async with self.redis_init_lock:
            if self.redis is not None:
                try:
                    await self.redis.ping()
                    return True
                except Exception:
                    logger.warning("Existing Redis connection is unavailable")
                    await self.close_redis()
            candidate = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                retry=REDIS_RETRY,
                retry_on_error=REDIS_RETRY_ERRORS,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )

            try:
                await candidate.ping()
                self.redis = candidate
                logger.info(
                    "Redis connection established (retry_count=%d, backoff=%ss->%ss, "
                    "health_check_interval=%ss)",
                    REDIS_RETRY_COUNT,
                    REDIS_BACKOFF_BASE,
                    REDIS_BACKOFF_CAP,
                    REDIS_HEALTH_CHECK_INTERVAL,
                )
                return True
            except Exception:
                logger.exception("Redis is unavailable")
                try:
                    await candidate.aclose()
                except Exception:
                    logger.exception("Failed to close failed Redis client")
                return False

    async def close_redis(self):
        if self.redis is not None:
            try:
                await self.redis.aclose()
            except Exception:
                logger.exception("Error while closing Redis")
            finally:
                self.redis = None

    def _connection_to_dict(self, connection: ConnectionInfo) -> dict:
        return {
            "connection_id": connection.connection_id,
            "producer_id": connection.producer_id,
            "default_stream_id": connection.default_stream_id,
            "created_at": connection.created_at,
            "last_activity": connection.last_activity,
            "streams": [
                {
                    "stream_id": stream.stream_id,
                    "stream_type": stream.stream_type,
                    "serialization_format": stream.serialization_format,
                    "meas_obj_dn": stream.meas_obj_dn,
                    "performance_metrics": stream.performance_metrics,
                    "vs_data_container": stream.vs_data_container,
                    "last_activity": stream.last_activity,
                }
                for stream in connection.streams.values()
            ],
        }

    def _dict_to_connection(self, data: dict) -> ConnectionInfo:
        connection = ConnectionInfo(
            connection_id=data["connection_id"],
            producer_id=data.get("producer_id", ""),
            default_stream_id=data.get("default_stream_id"),
            created_at=data.get("created_at", time.time()),
            last_activity=data.get("last_activity", time.time()),
        )
        for stream_data in data.get("streams", []):
            stream = StreamInfo(
                stream_id=stream_data["stream_id"],
                stream_type=stream_data["stream_type"],
                serialization_format=stream_data.get(
                    "serialization_format", "GPB"
                ),
                meas_obj_dn=stream_data.get("meas_obj_dn", ""),
                performance_metrics=stream_data.get("performance_metrics", []),
                vs_data_container=stream_data.get("vs_data_container", {}),
                last_activity=stream_data.get(
                    "last_activity", time.time()
                ),
            )
            connection.streams[stream.stream_id] = stream
        if connection.default_stream_id is None and connection.streams:
            connection.default_stream_id = next(iter(connection.streams))
        return connection

    async def _redis_set(self, connection: ConnectionInfo) -> bool:
        """Write metadata to Redis. Returns False if Redis is unavailable."""
        if not await self.init_redis():
            return False
        try:
            await self.redis.set(
                connection.connection_id,
                json.dumps(self._connection_to_dict(connection)),
                ex=CONNECTION_TTL,
            )
            return True
        except Exception:
            logger.exception(
                "Redis SET failed for %s",
                connection.connection_id,
            )
            await self.close_redis()
            return False

    async def _redis_get(self, connection_id: str) -> tuple[bool, Optional[ConnectionInfo]]:
        if not await self.init_redis():
            return False, None
        try:
            data = await self.redis.get(connection_id)
            if data is None:
                return True, None
            return True, self._dict_to_connection(json.loads(data))
        except Exception:
            logger.exception("Redis GET failed for %s", connection_id)
            await self.close_redis()
            return False, None

    async def _redis_delete(self, connection_id: str) -> bool:
        if not await self.init_redis():
            return False
        try:
            await self.redis.delete(connection_id)
            return True
        except Exception:
            logger.exception("Redis DELETE failed for %s", connection_id)
            await self.close_redis()
            return False

    def _store_pending_upsert(self, connection: ConnectionInfo):
        self.pending_deletes.discard(connection.connection_id)
        self.pending_upserts[connection.connection_id] = connection
        logger.warning(
            "Stored connection %s in local fallback memory",
            connection.connection_id,
        )

    def _store_pending_delete(self, connection_id: str):
        self.pending_upserts.pop(connection_id, None)
        self.pending_deletes.add(connection_id)
        logger.warning(
            "Stored delete for %s in local fallback memory",
            connection_id,
        )

    async def sync_pending_to_redis(self):
        """Push local fallback metadata to Redis and clear successful entries."""
        if not self.pending_upserts and not self.pending_deletes:
            return
        if not await self.init_redis():
            return
        for connection_id in list(self.pending_deletes):
            if await self._redis_delete(connection_id):
                self.pending_deletes.discard(connection_id)
                logger.info(
                    "Pending Redis delete synchronized for %s",
                    connection_id,
                )
            else:
                return
        for connection_id, connection in list(self.pending_upserts.items()):
            if await self._redis_set(connection):
                self.pending_upserts.pop(connection_id, None)
                logger.info(
                    "Pending Redis update synchronized for %s",
                    connection_id,
                )
            else:
                return

    async def monitor_redis(self):
        """Continuously keep trying to establish Redis connectivity."""
        delay = float(os.getenv("REDIS_RETRY_BASE", str(REDIS_BACKOFF_BASE)))
        cap = float(os.getenv("REDIS_RETRY_CAP", str(REDIS_BACKOFF_CAP)))

        while True:
            try:
                if self.redis is None:
                    if await self.init_redis():
                        delay = float(os.getenv("REDIS_RETRY_BASE", str(REDIS_BACKOFF_BASE)))
                    else:
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, cap)
                        continue
                else:
                    try:
                        await self.redis.ping()
                    except Exception:
                        await self.close_redis()
                await asyncio.sleep(REDIS_SYNC_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Redis monitor failed")
                await asyncio.sleep(delay)
                delay = min(delay * 2, cap)

    async def _redis_sync_loop(self):
        while True:
            try:
                await self.sync_pending_to_redis()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Redis recovery synchronization failed")
            await asyncio.sleep(REDIS_SYNC_INTERVAL)

    def start_redis_sync(self):
        if self.redis_sync_task is None or self.redis_sync_task.done():
            self.redis_sync_task = asyncio.create_task(
                self._redis_sync_loop()
            )

    async def stop_redis_sync(self):
        if self.redis_sync_task is not None:
            self.redis_sync_task.cancel()
            try:
                await self.redis_sync_task
            except asyncio.CancelledError:
                pass
            self.redis_sync_task = None
        try:
            await self.sync_pending_to_redis()
        except Exception:
            logger.exception("Final Redis synchronization failed")
        await self.close_redis()

    async def create_connection(
        self,
        stream_type: str,
        serialization_format: str = "GPB",
        stream_id: Optional[str] = None,
        producer_id: str = "",
        meas_obj_dn: str = "",
        performance_metrics: Optional[List[Any]] = None,
        vs_data_container: Optional[Dict[str, Any]] = None,
    ) -> ConnectionInfo:
        connection_id = str(uuid.uuid4())
        stream_id = stream_id or str(uuid.uuid4())
        stream = StreamInfo(
            stream_id=stream_id,
            stream_type=stream_type,
            serialization_format=serialization_format,
            meas_obj_dn=meas_obj_dn,
            performance_metrics=list(performance_metrics or []),
            vs_data_container=dict(vs_data_container or {}),
        )
        connection = ConnectionInfo(
            connection_id=connection_id,
            producer_id=producer_id,
            default_stream_id=stream_id,
        )
        connection.streams[stream_id] = stream
        await self.save_connection(connection)
        return connection

    async def save_connection(self, connection: ConnectionInfo) -> bool:
        """Persist metadata, retaining a process-local retry if Redis is down."""
        if await self._redis_set(connection):
            self.pending_upserts.pop(connection.connection_id, None)
            self.pending_deletes.discard(connection.connection_id)
            return True
        self._store_pending_upsert(connection)
        return False

    async def get_connection_lookup(
        self,
        connection_id: str,
    ) -> tuple[bool, Optional[ConnectionInfo]]:
        """Return (Redis reachable, metadata); distinguishes outage from missing ID."""
        redis_available, connection = await self._redis_get(connection_id)
        if connection_id in self.pending_deletes:
            return redis_available, None
        pending = self.pending_upserts.get(connection_id)
        if pending is not None:
            return redis_available, pending
        if redis_available:
            return True, connection
        return False, None

    async def get_connection(
        self,
        connection_id: str,
    ) -> Optional[ConnectionInfo]:
        _redis_available, connection = await self.get_connection_lookup(connection_id)
        return connection

    async def add_stream(
        self,
        connection_id: str,
        stream_type: str,
        serialization_format: str = "GPB",
        stream_id: Optional[str] = None,
        meas_obj_dn: str = "",
        performance_metrics: Optional[List[Any]] = None,
        vs_data_container: Optional[Dict[str, Any]] = None,
    ) -> Optional[StreamInfo]:
        connection = await self.get_connection(connection_id)
        if connection is None:
            return None
        stream_id = stream_id or str(uuid.uuid4())
        stream = StreamInfo(
            stream_id=stream_id,
            stream_type=stream_type,
            serialization_format=serialization_format,
            meas_obj_dn=meas_obj_dn,
            performance_metrics=list(performance_metrics or []),
            vs_data_container=dict(vs_data_container or {}),
        )
        connection.streams[stream_id] = stream
        connection.last_activity = time.time()
        await self.save_connection(connection)
        return stream

    async def get_stream(
        self,
        connection_id: str,
        stream_id: str,
    ) -> Optional[StreamInfo]:
        connection = await self.get_connection(connection_id)
        if connection is None:
            return None
        return connection.streams.get(stream_id)

    async def update_activity(
        self,
        connection_id: str,
        stream_id: Optional[str] = None,
    ):
        connection = await self.get_connection(connection_id)
        if connection is None:
            return False
        now = time.time()
        connection.last_activity = now
        if stream_id:
            stream = connection.streams.get(stream_id)
            if stream:
                stream.last_activity = now
        if not await self._redis_set(connection):
            self._store_pending_upsert(connection)
        return True

    async def delete_stream(
        self,
        connection_id: str,
        stream_id: str,
    ) -> bool:
        connection = await self.get_connection(connection_id)
        if connection is None:
            return False
        if stream_id not in connection.streams:
            return False
        if connection.default_stream_id == stream_id:
            return False

        del connection.streams[stream_id]
        connection.last_activity = time.time()
        await self.save_connection(connection)
        return True

    async def delete_connection(self, connection_id: str) -> bool:
        connection = await self.get_connection(connection_id)
        if connection is None:
            return False
        if not await self._redis_delete(connection_id):
            self._store_pending_delete(connection_id)
        else:
            self.pending_upserts.pop(connection_id, None)
            self.pending_deletes.discard(connection_id)
        return True

def validate_stream_request(request: StreamInfoRequest) -> str:
    """Normalize stream type; detailed schema validation is done by Pydantic."""
    stream_type = request.stream_type.strip().upper()
    return "PERFORMANCE" if stream_type == "PERF3GPP" else stream_type

def connection_response(connection: ConnectionInfo) -> dict:
    """Build a JSON-safe representation using the legacy stream field names."""
    return {
        "connection_id": connection.connection_id,
        "producerId": connection.producer_id,
        "info": {
            stream_id: stream_response_fields(stream)
            for stream_id, stream in connection.streams.items()
        },
    }

def stream_response_fields(stream: StreamInfo) -> dict:
    fields = {"StreamType": stream.stream_type}
    if stream.stream_type == "PROPRIETARY":
        fields["VsDataContainer"] = stream.vs_data_container
    else:
        fields["performanceMetrics"] = stream.performance_metrics
    return fields

def stream_response(connection_id: str, stream: StreamInfo) -> dict:
    """Build the public API representation of a stream."""
    return {
        "connection_id": connection_id,
        "stream_id": stream.stream_id,
        "stream_type": stream.stream_type,
        "serialization_format": stream.serialization_format,
    }


# ---------------------------------------------------------------------------
# Kafka utilities
# ---------------------------------------------------------------------------

from pathlib import Path as _Path
from aiokafka import AIOKafkaProducer

PERF_KAFKA_TOPIC = os.getenv("PERF_KAFKA_TOPIC", "HV_VES_PERF3GPP")
LOG_KAFKA_TOPIC = os.getenv("LOG_KAFKA_TOPIC", "HV_VES_PROPRIETARY")
KAFKA_CONFIG_FILE = os.getenv("KAFKA_CONFIG_FILE", "meta.json")
kafka_bootstrap_server = ""
kafka_user = ""
kafka_password = ""
kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
kafka_producer: Optional[AIOKafkaProducer] = None
kafka_start_lock = asyncio.Lock()


def get_kafka_configs() -> None:
    global kafka_bootstrap_server, kafka_user, kafka_password
    global kafka_security_protocol, kafka_sasl_mechanism
    kafka_bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    kafka_user = os.getenv("KAFKA_USERNAME", "")
    kafka_password = os.getenv("KAFKA_PASSWORD", "")
    kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", kafka_security_protocol)
    kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", kafka_sasl_mechanism)
    config_path = _Path(KAFKA_CONFIG_FILE)
    if not config_path.is_absolute() and not config_path.exists():
        project_root_config = _Path(__file__).resolve().parent.parent / config_path
        if project_root_config.exists():
            config_path = project_root_config
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as file:
            config = json.load(file)
        kafka_config = config.get("kafka", {})
        kafka_bootstrap_server = kafka_bootstrap_server or kafka_config.get("bootstrap_server", "")
        kafka_user = kafka_user or kafka_config.get("username", "")
        kafka_password = kafka_password or kafka_config.get("password", "")
        kafka_security_protocol = os.getenv(
            "KAFKA_SECURITY_PROTOCOL",
            kafka_config.get("security_protocol", kafka_security_protocol),
        )
        kafka_sasl_mechanism = os.getenv(
            "KAFKA_SASL_MECHANISM",
            kafka_config.get("sasl_mechanism", kafka_sasl_mechanism),
        )
    elif not kafka_bootstrap_server:
        raise FileNotFoundError(
            f"Kafka config not found ({KAFKA_CONFIG_FILE}) and KAFKA_BOOTSTRAP_SERVERS is unset"
        )
    if not kafka_bootstrap_server:
        raise ValueError("Kafka bootstrap_server is not configured")
    logger.info("Kafka configuration loaded. bootstrap_server=%s", kafka_bootstrap_server)


async def start_kafka() -> None:
    """Start one long-lived producer; concurrent callers share the same instance."""
    global kafka_producer
    async with kafka_start_lock:
        if kafka_producer is not None:
            return
        get_kafka_configs()
        producer_options = {
            "bootstrap_servers": kafka_bootstrap_server,
            "security_protocol": kafka_security_protocol,
            "compression_type": "lz4",
            "linger_ms": 20,
            "max_batch_size": 131072,
            "max_request_size": 10485760,
            "acks": 1,
            "retry_backoff_ms": 500,
            "request_timeout_ms": 30000,
            "metadata_max_age_ms": 300000,
            "connections_max_idle_ms": 540000,
            "enable_idempotence": False,
        }
        if kafka_security_protocol.startswith("SASL_"):
            if not kafka_user or not kafka_password:
                raise ValueError("KAFKA_USERNAME and KAFKA_PASSWORD are required for SASL Kafka")
            producer_options.update({
                "sasl_mechanism": kafka_sasl_mechanism,
                "sasl_plain_username": kafka_user,
                "sasl_plain_password": kafka_password,
            })
        producer = AIOKafkaProducer(**producer_options)
        try:
            await producer.start()
            kafka_producer = producer
            logger.info("Long-lived AIOKafkaProducer started")
        except Exception:
            try:
                await producer.stop()
            except Exception:
                logger.exception("Failed to close Kafka producer after startup failure")
            kafka_producer = None
            raise


async def connect_kafka_with_retry() -> None:
    """Keep the Kafka producer connected and reconnect after failures."""
    delay = float(os.getenv("KAFKA_RETRY_BASE", "1"))
    cap = float(os.getenv("KAFKA_RETRY_CAP", "10"))
    while True:
        try:
            if kafka_producer is None:
                try:
                    await start_kafka()
                    delay = float(os.getenv("KAFKA_RETRY_BASE", "1"))
                except Exception:
                    logger.exception(
                        "Kafka unavailable; retrying in %.1f seconds", delay
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, cap)
                    continue

            await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Kafka monitor failed")
            await asyncio.sleep(delay)
            delay = min(delay * 2, cap)


async def stop_kafka() -> None:
    """Stop the long-lived Kafka producer for this worker process."""
    global kafka_producer

    if kafka_producer is None:
        return

    producer = kafka_producer
    kafka_producer = None
    try:
        await producer.stop()
        logger.info("Kafka producer stopped")
    except Exception:
        logger.exception("Error while stopping Kafka producer")


def get_kafka_topic(stream_type: str) -> str:
    """Return the Kafka topic for a VES stream type."""
    if stream_type.strip().upper() == "PROPRIETARY":
        return LOG_KAFKA_TOPIC
    return PERF_KAFKA_TOPIC


async def write_packet(packet: bytes, stream_type: str) -> None:
    """Send a raw VES packet with producer and reconnect retries."""
    global kafka_producer

    if not isinstance(packet, bytes):
        raise TypeError("Kafka packet must be bytes")

    topic = get_kafka_topic(stream_type)
    send_retries = int(os.getenv("KAFKA_SEND_RETRY_COUNT", "5"))
    base_delay = float(os.getenv("KAFKA_SEND_RETRY_BASE", "0.5"))
    cap_delay = float(os.getenv("KAFKA_SEND_RETRY_CAP", "10"))
    delay = base_delay

    for attempt in range(1, send_retries + 1):
        producer = kafka_producer

        if producer is None:
            try:
                await start_kafka()
                producer = kafka_producer
            except Exception:
                logger.exception(
                    "Kafka connection attempt %d/%d failed for topic=%s",
                    attempt,
                    send_retries,
                    topic,
                )

        if producer is not None:
            try:
                await producer.send_and_wait(
                    topic=topic,
                    value=packet,
                )
                return
            except Exception:
                logger.exception(
                    "Kafka send attempt %d/%d failed for topic=%s",
                    attempt,
                    send_retries,
                    topic,
                )
                if kafka_producer is producer:
                    try:
                        await producer.stop()
                    except Exception:
                        logger.exception(
                            "Failed to stop failed Kafka producer"
                        )
                    kafka_producer = None

        if attempt < send_retries:
            await asyncio.sleep(delay)
            delay = min(delay * 2, cap_delay)

    raise RuntimeError(
        f"Kafka unavailable after {send_retries} send attempts for topic={topic}"
    )

