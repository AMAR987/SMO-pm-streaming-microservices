import asyncio
import base64
import json
import logging
import os
import time
import uuid

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator
from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry
from aiokafka import AIOKafkaProducer


logger = logging.getLogger("hv-ves")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RedisPersistenceError(Exception):
    """Raised when a required Redis persistence operation fails."""


# ---------------------------------------------------------------------------
# Stream request models
# ---------------------------------------------------------------------------

class StreamInfoRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    stream_id: str = Field(..., alias="streamId", min_length=1)
    stream_type: str = Field(..., alias="StreamType", min_length=1)
    serialization_format: str = Field("GPB", alias="SerializationFormat")
    meas_obj_dn: str = Field("", alias="measObjDn")
    performance_metrics: List[Any] = Field(
        default_factory=list,
        alias="performanceMetrics",
    )
    vs_data_container: Dict[str, Any] = Field(
        default_factory=dict,
        alias="VsDataContainer",
    )

    @model_validator(mode="before")
    @classmethod
    def unwrap_legacy_stream_wrapper(cls, values):
        if isinstance(values, dict) and "streamInfoList" in values:
            wrapped = values["streamInfoList"]

            if isinstance(wrapped, list):
                if len(wrapped) != 1:
                    raise ValueError(
                        "streamInfoList must contain exactly one stream "
                        "for this endpoint"
                    )
                wrapped = wrapped[0]

            if not isinstance(wrapped, dict):
                raise ValueError("streamInfoList must be a stream object")

            values = wrapped

        return values

    @model_validator(mode="after")
    def validate_stream_details(self):
        stream_type = self.stream_type.strip().upper()

        if stream_type not in {
            "PERFORMANCE",
            "PERF3GPP",
            "PROPRIETARY",
        }:
            raise ValueError(
                "StreamType must be PERFORMANCE, PERF3GPP, or PROPRIETARY"
            )

        if self.serialization_format.strip().upper() != "GPB":
            raise ValueError("Only GPB SerializationFormat is supported")

        self.stream_type = (
            "PROPRIETARY"
            if stream_type == "PROPRIETARY"
            else "PERFORMANCE"
        )
        self.serialization_format = "GPB"

        return self


class ConnectionRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    producer_id: str = Field(..., alias="producerId", min_length=1)
    stream_info_list: List[StreamInfoRequest] = Field(
        ...,
        alias="streamInfoList",
        min_length=1,
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_stream_info_list(cls, values):
        if not isinstance(values, dict):
            return values

        stream_info = values.get(
            "streamInfoList",
            values.get("stream_info_list"),
        )

        if isinstance(stream_info, dict):
            values = dict(values)
            values["streamInfoList"] = [stream_info]

        return values


class StreamRequest(StreamInfoRequest):
    """Request model for adding one stream."""


# ---------------------------------------------------------------------------
# Redis configuration
# ---------------------------------------------------------------------------

CONNECTION_TTL = max(0, int(os.getenv("CONNECTION_TTL", "300")))

REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
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
REDIS_SOCKET_TIMEOUT = float(
    os.getenv("REDIS_SOCKET_TIMEOUT", "5")
)

REDIS_RETRY_ERRORS = [
    RedisConnectionError,
    RedisTimeoutError,
    ConnectionResetError,
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StreamInfo:
    stream_id: str
    stream_type: str = "PERFORMANCE"
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
    streams_metaData: Dict[str, Any] = field(default_factory=dict)
    hash_map: Dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------

PERF_KAFKA_TOPIC = os.getenv(
    "PERF_KAFKA_TOPIC",
    "HV_VES_PERF3GPP",
)
PROPRIETARY_KAFKA_TOPIC = os.getenv(
    "PROPRIETARY_KAFKA_TOPIC",
    "HV_VES_PROPRIETARY",
)

KAFKA_PENDING_QUEUE_KEY = os.getenv(
    "KAFKA_PENDING_QUEUE_KEY",
    "hvves:kafka:pending",
)
KAFKA_PROCESSING_QUEUE_KEY = os.getenv(
    "KAFKA_PROCESSING_QUEUE_KEY",
    "hvves:kafka:processing",
)

KAFKA_CONFIG_FILE = os.getenv("KAFKA_CONFIG_FILE", "meta.json")

kafka_bootstrap_server = ""
kafka_user = ""
kafka_password = ""

kafka_security_protocol = os.getenv(
    "KAFKA_SECURITY_PROTOCOL",
    "PLAINTEXT",
)
kafka_sasl_mechanism = os.getenv(
    "KAFKA_SASL_MECHANISM",
    "PLAIN",
)

kafka_producer: Optional[AIOKafkaProducer] = None
kafka_start_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Redis client manager
# ---------------------------------------------------------------------------

class ClientInfo:

    def __init__(self):
        self.redis: Optional[Redis] = None
        self.redis_sync_task: Optional[asyncio.Task] = None
        self.redis_init_lock = asyncio.Lock()

        # Local metadata cache is independent of Redis.
        # It contains metadata loaded from Redis and metadata created locally.
        self.connection_cache: Dict[str, ConnectionInfo] = {}

        # Metadata waiting to be synchronized with Redis.
        self.pending_upserts: Dict[str, ConnectionInfo] = {}
        self.pending_deletes: set[str] = set()

    def _redis_key(self, connection_id: str) -> str:
        return f"{REDIS_KEY_PREFIX}{connection_id}"

    # -----------------------------------------------------------------------
    # Redis connection
    # -----------------------------------------------------------------------

    async def init_redis(self) -> bool:
        async with self.redis_init_lock:

            if self.redis is not None:
                return True

            candidate = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                retry=Retry(
                    ExponentialBackoff(
                        cap=REDIS_BACKOFF_CAP,
                        base=REDIS_BACKOFF_BASE,
                    ),
                    REDIS_RETRY_COUNT,
                ),
                retry_on_error=REDIS_RETRY_ERRORS,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )

            try:
                await candidate.ping()
                self.redis = candidate

                logger.info(
                    "Redis connection established "
                    "| Host=%s | Port=%s | DB=%s",
                    REDIS_HOST,
                    REDIS_PORT,
                    REDIS_DB,
                )

                return True

            except Exception:
                logger.warning(
                    "Redis unavailable at %s:%s; local metadata cache active",
                    REDIS_HOST,
                    REDIS_PORT,
                )

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

    async def redis_status(self) -> dict:
        connected = False

        if self.redis is not None:
            try:
                await self.redis.ping()
                connected = True
            except Exception:
                logger.warning("Redis health check failed")
                await self.close_redis()

        return {
            "connected": connected,
            "db": REDIS_DB,
        }

    # -----------------------------------------------------------------------
    # Connection serialization
    # -----------------------------------------------------------------------

    def _connection_to_dict(
        self,
        connection: ConnectionInfo,
    ) -> dict:

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
                stream_type=stream_data.get(
                    "stream_type",
                    "PERFORMANCE",
                ),
                serialization_format=stream_data.get(
                    "serialization_format",
                    "GPB",
                ),
                meas_obj_dn=stream_data.get("meas_obj_dn", ""),
                performance_metrics=stream_data.get(
                    "performance_metrics",
                    [],
                ),
                vs_data_container=stream_data.get(
                    "vs_data_container",
                    {},
                ),
                last_activity=stream_data.get(
                    "last_activity",
                    time.time(),
                ),
            )

            connection.streams[stream.stream_id] = stream

        if connection.default_stream_id is None and connection.streams:
            connection.default_stream_id = next(iter(connection.streams))

        return connection

    # -----------------------------------------------------------------------
    # Redis metadata operations
    # -----------------------------------------------------------------------

    async def _redis_set(
        self,
        connection: ConnectionInfo,
    ) -> bool:

        try:
            payload = json.dumps(
                self._connection_to_dict(connection),
                separators=(",", ":"),
            )
        except (TypeError, ValueError):
            logger.exception(
                "Connection metadata for %s is not JSON serializable",
                connection.connection_id,
            )
            return False

        if not await self.init_redis():
            return False

        try:
            key = self._redis_key(connection.connection_id)

            # Keep the configured TTL at 300 seconds.
            if CONNECTION_TTL > 0:
                await self.redis.set(
                    key,
                    payload,
                    ex=CONNECTION_TTL,
                )
                ttl_label = f"{CONNECTION_TTL}s"
            else:
                await self.redis.set(key, payload)
                ttl_label = "NEVER"

            logger.info(
                "REDIS_WRITE_OK key=%s db=%s ttl=%s bytes=%s",
                key,
                REDIS_DB,
                ttl_label,
                len(payload.encode("utf-8")),
            )

            return True

        except Exception:
            logger.exception(
                "Redis SET failed for %s",
                connection.connection_id,
            )
            await self.close_redis()
            return False

    async def _redis_get(
        self,
        connection_id: str,
    ) -> tuple[bool, Optional[ConnectionInfo]]:

        if not await self.init_redis():
            return False, None

        try:
            data = await self.redis.get(
                self._redis_key(connection_id)
            )

            if data is None:
                return True, None

            return True, self._dict_to_connection(json.loads(data))

        except Exception:
            logger.exception(
                "Redis GET failed for %s",
                connection_id,
            )
            await self.close_redis()
            return False, None

    async def _redis_delete(
        self,
        connection_id: str,
    ) -> bool:

        if not await self.init_redis():
            return False

        try:
            await self.redis.delete(
                self._redis_key(connection_id)
            )
            return True

        except Exception:
            logger.exception(
                "Redis DELETE failed for %s",
                connection_id,
            )
            await self.close_redis()
            return False

    # -----------------------------------------------------------------------
    # Local metadata fallback
    # -----------------------------------------------------------------------

    def _store_pending_upsert(
        self,
        connection: ConnectionInfo,
    ):

        connection_id = connection.connection_id

        self.pending_deletes.discard(connection_id)

        # Always keep the latest connection metadata locally.
        self.connection_cache[connection_id] = connection

        # Retry Redis synchronization when Redis becomes available.
        self.pending_upserts[connection_id] = connection

        logger.warning(
            "Connection metadata stored in local fallback cache "
            "| ConnectionID=%s",
            connection_id,
        )

    def _store_pending_delete(
        self,
        connection_id: str,
    ):

        self.pending_upserts.pop(connection_id, None)
        self.connection_cache.pop(connection_id, None)
        self.pending_deletes.add(connection_id)

        logger.warning(
            "Connection deletion stored for Redis synchronization "
            "| ConnectionID=%s",
            connection_id,
        )

    # -----------------------------------------------------------------------
    # Save and retrieve connection metadata
    # -----------------------------------------------------------------------

    async def save_connection(
        self,
        connection: ConnectionInfo,
    ) -> bool:

        connection_id = connection.connection_id

        # IMPORTANT: Cache metadata before attempting Redis.
        self.connection_cache[connection_id] = connection

        if await self._redis_set(connection):

            self.pending_upserts.pop(connection_id, None)
            self.pending_deletes.discard(connection_id)

            logger.info(
                "Connection metadata saved to Redis and local cache "
                "| ConnectionID=%s",
                connection_id,
            )

            return True

        self._store_pending_upsert(connection)

        return False

    async def get_connection_lookup(
        self,
        connection_id: str,
    ) -> tuple[bool, Optional[ConnectionInfo]]:

        if connection_id in self.pending_deletes:
            return self.redis is not None, None

        # 1. Always check local metadata first.
        cached = self.connection_cache.get(connection_id)

        if cached is not None:
            return self.redis is not None, cached

        # 2. Check pending Redis writes.
        pending = self.pending_upserts.get(connection_id)

        if pending is not None:
            self.connection_cache[connection_id] = pending
            return self.redis is not None, pending

        # 3. Only query Redis if local metadata is unavailable.
        redis_available, connection = await self._redis_get(
            connection_id
        )

        if connection is not None:

            # Cache metadata obtained from Redis.
            self.connection_cache[connection_id] = connection

            logger.info(
                "[CONNECTION] Metadata loaded from Redis "
                "| ConnectionID=%s | Streams=%s",
                connection_id,
                list(connection.streams.keys()),
            )

            return True, connection

        if redis_available:
            logger.warning(
                "[CONNECTION] Metadata not found "
                "| ConnectionID=%s | Source=Redis",
                connection_id,
            )
        else:
            logger.error(
                "[CONNECTION] Redis unavailable and no local metadata "
                "| ConnectionID=%s",
                connection_id,
            )

        return redis_available, None

    async def get_connection(
        self,
        connection_id: str,
    ) -> Optional[ConnectionInfo]:

        _redis_available, connection = (
            await self.get_connection_lookup(connection_id)
        )

        return connection

    # -----------------------------------------------------------------------
    # Redis synchronization
    # -----------------------------------------------------------------------

    async def sync_pending_to_redis(self):

        if not self.pending_upserts and not self.pending_deletes:
            return

        if not await self.init_redis():
            return

        for connection_id in list(self.pending_deletes):

            if await self._redis_delete(connection_id):

                self.pending_deletes.discard(connection_id)

                logger.info(
                    "Pending Redis delete synchronized "
                    "| ConnectionID=%s",
                    connection_id,
                )

            else:
                return

        for connection_id, connection in list(
            self.pending_upserts.items()
        ):

            if await self._redis_set(connection):

                if self.pending_upserts.get(connection_id) is connection:
                    self.pending_upserts.pop(connection_id, None)

                logger.info(
                    "Pending Redis update synchronized "
                    "| ConnectionID=%s",
                    connection_id,
                )

            else:
                return

    async def monitor_redis(self):

        delay = REDIS_BACKOFF_BASE

        while True:
            try:

                if self.redis is None:

                    if await self.init_redis():
                        delay = REDIS_BACKOFF_BASE
                        await self.sync_pending_to_redis()
                    else:
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, REDIS_BACKOFF_CAP)
                        continue

                else:
                    try:
                        await self.redis.ping()
                        await self.sync_pending_to_redis()

                    except Exception:
                        logger.warning(
                            "Redis connection lost; local cache remains active"
                        )
                        await self.close_redis()

                await asyncio.sleep(REDIS_SYNC_INTERVAL)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Redis monitor failed")
                await asyncio.sleep(delay)
                delay = min(delay * 2, REDIS_BACKOFF_CAP)

    # -----------------------------------------------------------------------
    # Redis queue operations for Kafka
    # -----------------------------------------------------------------------

    async def enqueue_kafka_packet(
        self,
        packet: bytes,
        connection_id: str,
        topic: str,
    ) -> bool:

        if not await self.init_redis():
            return False

        record = json.dumps(
            {
                "packet": base64.b64encode(packet).decode("ascii"),
                "connection_id": connection_id,
                "topic": topic,
                "created_at": time.time(),
            },
            separators=(",", ":"),
        )

        try:
            await self.redis.lpush(
                KAFKA_PENDING_QUEUE_KEY,
                record,
            )

            logger.warning(
                "Packet queued in Redis "
                "| ConnectionID=%s | Topic=%s | Bytes=%s",
                connection_id,
                topic,
                len(packet),
            )

            return True

        except Exception:
            logger.exception(
                "Redis packet enqueue failed "
                "| ConnectionID=%s",
                connection_id,
            )
            await self.close_redis()
            return False

    async def claim_kafka_packet(self):

        if not await self.init_redis():
            return None

        try:
            return await self.redis.rpoplpush(
                KAFKA_PENDING_QUEUE_KEY,
                KAFKA_PROCESSING_QUEUE_KEY,
            )

        except Exception:
            logger.exception("Could not claim queued Kafka packet")
            await self.close_redis()
            return None

    async def acknowledge_kafka_packet(self, record: str):

        if await self.init_redis():
            try:
                await self.redis.lrem(
                    KAFKA_PROCESSING_QUEUE_KEY,
                    1,
                    record,
                )
            except Exception:
                logger.exception("Kafka queue acknowledgement failed")
                await self.close_redis()

    async def requeue_kafka_packet(self, record: str):

        if await self.init_redis():
            try:
                await self.redis.lrem(
                    KAFKA_PROCESSING_QUEUE_KEY,
                    1,
                    record,
                )
                await self.redis.lpush(
                    KAFKA_PENDING_QUEUE_KEY,
                    record,
                )
            except Exception:
                logger.exception("Kafka packet requeue failed")
                await self.close_redis()

    async def recover_kafka_queue(self):

        if not await self.init_redis():
            return

        try:
            while True:
                record = await self.redis.rpoplpush(
                    KAFKA_PROCESSING_QUEUE_KEY,
                    KAFKA_PENDING_QUEUE_KEY,
                )

                if record is None:
                    break

            logger.info("Recovered unacknowledged Kafka queue records")

        except Exception:
            logger.exception("Kafka queue recovery failed")
            await self.close_redis()

    async def log_existing_connections(self) -> int:

        if not await self.init_redis():
            logger.warning(
                "[STARTUP][REDIS] Inventory skipped: Redis unavailable"
            )
            return 0

        logger.info("[STARTUP][REDIS] Checking existing connection records")

        count = 0
        cursor = 0
        pattern = f"{REDIS_KEY_PREFIX}*"

        try:
            while True:

                cursor, keys = await self.redis.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100,
                )

                for key in keys:
                    try:
                        raw = await self.redis.get(key)

                        if not raw:
                            continue

                        connection = self._dict_to_connection(
                            json.loads(raw)
                        )

                        # Populate local cache at startup.
                        self.connection_cache[
                            connection.connection_id
                        ] = connection

                        ttl = await self.redis.ttl(key)

                        logger.info(
                            "[STARTUP][CONNECTION] EXISTING CONNECTION FOUND "
                            "| ConnectionID=%s | ProducerID=%s "
                            "| DefaultStreamID=%s | StreamCount=%s "
                            "| StreamIDs=%s | RedisTTL=%ss",
                            connection.connection_id,
                            connection.producer_id or "N/A",
                            connection.default_stream_id or "N/A",
                            len(connection.streams),
                            list(connection.streams.keys()),
                            ttl,
                        )

                        count += 1

                    except Exception:
                        logger.exception(
                            "[STARTUP][REDIS] Failed to read connection "
                            "| Key=%s",
                            key,
                        )

                if cursor == 0:
                    break

            logger.info(
                "[STARTUP][REDIS] Inventory complete | Total=%s",
                count,
            )

            return count

        except Exception:
            logger.exception("[STARTUP][REDIS] Inventory failed")
            await self.close_redis()
            return count

    # -----------------------------------------------------------------------
    # Connection lifecycle
    # -----------------------------------------------------------------------

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

        connection_id = uuid.uuid4().hex[:15]
        stream_id = stream_id or str(uuid.uuid4())

        stream = StreamInfo(
            stream_id=stream_id,
            stream_type=stream_type.strip().upper(),
            serialization_format=serialization_format.strip().upper(),
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

        # save_connection caches locally before attempting Redis.
        await self.save_connection(connection)

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
            stream_type=stream_type.strip().upper(),
            serialization_format=serialization_format.strip().upper(),
            meas_obj_dn=meas_obj_dn,
            performance_metrics=list(performance_metrics or []),
            vs_data_container=dict(vs_data_container or {}),
        )

        connection.streams[stream_id] = stream
        connection.last_activity = time.time()

        await self.save_connection(connection)

        logger.info(
            "Stream metadata updated "
            "| ConnectionID=%s | StreamID=%s",
            connection_id,
            stream_id,
        )

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

        await self.save_connection(connection)

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

    async def delete_connection(
        self,
        connection_id: str,
    ) -> bool:

        connection = await self.get_connection(connection_id)

        if connection is None:
            return False

        # Explicit API deletion removes local metadata.
        self.connection_cache.pop(connection_id, None)

        if not await self._redis_delete(connection_id):
            self._store_pending_delete(connection_id)
        else:
            self.pending_upserts.pop(connection_id, None)
            self.pending_deletes.discard(connection_id)

        return True

    # -----------------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------------

    async def stop_redis_sync(self):

        try:
            await self.sync_pending_to_redis()
        except Exception:
            logger.exception("Final Redis synchronization failed")

        await self.close_redis()


# ---------------------------------------------------------------------------
# API response helpers
# ---------------------------------------------------------------------------

def validate_stream_request(request: StreamInfoRequest) -> str:
    return request.stream_type


def connection_response(connection: ConnectionInfo) -> dict:
    return {
        "connection_id": connection.connection_id,
        "producerId": connection.producer_id,
        "info": {
            stream_id: stream_response_fields(stream)
            for stream_id, stream in connection.streams.items()
        },
    }


def stream_response_fields(stream: StreamInfo) -> dict:
    return {
        "StreamType": stream.stream_type,
        "performanceMetrics": stream.performance_metrics,
    }


def stream_response(
    connection_id: str,
    stream: StreamInfo,
) -> dict:
    return {
        "connection_id": connection_id,
        "stream_id": stream.stream_id,
        "stream_type": stream.stream_type,
        "serialization_format": stream.serialization_format,
    }


# ---------------------------------------------------------------------------
# Legacy compatibility helpers
# ---------------------------------------------------------------------------

STATUS_BAD_REQUEST = 400
STATUS_OK = 200
STATUS_ACCEPTED = 201
STATUS_SERVER_ERROR = 503
STATUS_FORBIDDEN = 403
STATUS_SWITCH_PROTOCOL = 101


def allocate_connection_id() -> str:
    return uuid.uuid4().hex[:15]


def validate_data(data: dict) -> bool:
    try:
        if not isinstance(data, dict):
            return False

        if not data.get("producerId"):
            return False

        stream_info = data.get("streamInfoList")

        if isinstance(stream_info, list):
            if len(stream_info) < 1:
                return False
            stream_info = stream_info[0]

        if not isinstance(stream_info, dict):
            return False

        stream_type = str(
            stream_info.get("StreamType", "")
        ).strip().upper()

        serialization = str(
            stream_info.get("SerializationFormat", "GPB")
        ).strip().upper()

        if stream_type not in {
            "PERFORMANCE",
            "PERF3GPP",
            "PROPRIETARY",
        }:
            return False

        if serialization != "GPB":
            return False

        return isinstance(
            stream_info.get("performanceMetrics", []),
            list,
        )

    except (TypeError, ValueError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------

def get_kafka_configs() -> None:
    global kafka_bootstrap_server
    global kafka_user
    global kafka_password
    global kafka_security_protocol
    global kafka_sasl_mechanism

    kafka_bootstrap_server = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "",
    ).strip()

    kafka_user = os.getenv("KAFKA_USERNAME", "")
    kafka_password = os.getenv("KAFKA_PASSWORD", "")

    kafka_security_protocol = os.getenv(
        "KAFKA_SECURITY_PROTOCOL",
        kafka_security_protocol,
    )

    kafka_sasl_mechanism = os.getenv(
        "KAFKA_SASL_MECHANISM",
        kafka_sasl_mechanism,
    )

    config_path = Path(KAFKA_CONFIG_FILE)

    if not config_path.is_absolute() and not config_path.exists():

        candidates = [
            Path(__file__).resolve().parent / config_path,
            Path(__file__).resolve().parent.parent / config_path,
            Path(__file__).resolve().parent.parent.parent / config_path,
        ]

        for candidate in candidates:
            if candidate.exists():
                config_path = candidate
                break

    if config_path.exists():

        with config_path.open("r", encoding="utf-8") as file:
            config = json.load(file)

        kafka_config = config.get("kafka", {})

        kafka_bootstrap_server = (
            kafka_bootstrap_server
            or kafka_config.get("bootstrap_server", "")
        )

        kafka_user = kafka_user or kafka_config.get("username", "")
        kafka_password = kafka_password or kafka_config.get("password", "")

        kafka_security_protocol = os.getenv(
            "KAFKA_SECURITY_PROTOCOL",
            kafka_config.get(
                "security_protocol",
                kafka_security_protocol,
            ),
        )

        kafka_sasl_mechanism = os.getenv(
            "KAFKA_SASL_MECHANISM",
            kafka_config.get(
                "sasl_mechanism",
                kafka_sasl_mechanism,
            ),
        )

    elif not kafka_bootstrap_server:
        raise FileNotFoundError(
            f"Kafka config not found ({KAFKA_CONFIG_FILE}) "
            "and KAFKA_BOOTSTRAP_SERVERS is unset"
        )

    if not kafka_bootstrap_server:
        raise ValueError("Kafka bootstrap_server is not configured")

    logger.info(
        "Kafka configuration loaded | BootstrapServer=%s",
        kafka_bootstrap_server,
    )


# ---------------------------------------------------------------------------
# Kafka producer lifecycle
# ---------------------------------------------------------------------------

async def start_kafka() -> None:
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
                raise ValueError(
                    "KAFKA_USERNAME and KAFKA_PASSWORD "
                    "are required for SASL Kafka"
                )

            producer_options.update({
                "sasl_mechanism": kafka_sasl_mechanism,
                "sasl_plain_username": kafka_user,
                "sasl_plain_password": kafka_password,
            })

        producer = AIOKafkaProducer(**producer_options)

        try:
            await producer.start()
            kafka_producer = producer

            logger.info(
                "Kafka producer started | PID=%s | Topic=%s",
                os.getpid(),
                PERF_KAFKA_TOPIC,
            )

        except Exception:
            try:
                await producer.stop()
            except Exception:
                logger.exception(
                    "Failed to close Kafka producer after startup failure"
                )

            kafka_producer = None
            raise


async def connect_kafka_with_retry() -> None:

    delay = float(os.getenv("KAFKA_RETRY_BASE", "1"))
    cap = float(os.getenv("KAFKA_RETRY_CAP", "10"))

    while True:
        try:

            if kafka_producer is None:
                try:
                    await start_kafka()
                    delay = float(os.getenv("KAFKA_RETRY_BASE", "1"))

                except Exception as exc:
                    logger.warning(
                        "Kafka unavailable (%s); retrying in %.1f seconds",
                        exc,
                        delay,
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
    global kafka_producer

    if kafka_producer is None:
        return

    producer = kafka_producer
    kafka_producer = None

    try:
        await producer.stop()

        logger.info(
            "Kafka producer stopped | PID=%s",
            os.getpid(),
        )

    except Exception:
        logger.exception("Error while stopping Kafka producer")


# ---------------------------------------------------------------------------
# Raw Kafka packet publishing
# ---------------------------------------------------------------------------


async def write_packet(
    packet: bytes,
    connection_id: str,
    topic: Optional[str] = None,
) -> None:

    global kafka_producer

    if not isinstance(packet, bytes):
        raise TypeError("Kafka packet must be bytes")

    if not connection_id:
        raise ValueError("connection_id is required")

    topic = topic or PERF_KAFKA_TOPIC
    message_key = connection_id.encode("utf-8")

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
                    "Kafka connection attempt %d/%d failed "
                    "| PID=%s | ConnectionID=%s",
                    attempt,
                    send_retries,
                    os.getpid(),
                    connection_id,
                )

        if producer is not None:
            try:
                metadata = await producer.send_and_wait(
                    topic=topic,
                    key=message_key,
                    value=packet,
                )

                logger.info(
                    "VES packet published "
                    "| PID=%s | ConnectionID=%s | Topic=%s "
                    "| Partition=%s | Offset=%s | PacketSize=%s bytes",
                    os.getpid(),
                    connection_id,
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                    len(packet),
                )

                return

            except Exception:
                logger.exception(
                    "Kafka send attempt %d/%d failed "
                    "| PID=%s | Topic=%s | ConnectionID=%s",
                    attempt,
                    send_retries,
                    os.getpid(),
                    topic,
                    connection_id,
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
        f"Kafka unavailable after {send_retries} "
        f"send attempts for topic={topic}"
    )



# ---------------------------------------------------------------------------
# Compatibility helpers
# ---------------------------------------------------------------------------

async def init_kafka() -> None:
    await start_kafka()


async def close_kafka() -> None:
    await stop_kafka()


async def async_kafka_send(
    data,
    connection_id: str,
) -> None:

    if isinstance(data, bytes):
        packet = data

    elif isinstance(data, bytearray):
        packet = bytes(data)

    else:
        serializer = getattr(data, "SerializeToString", None)

        if not callable(serializer):
            raise TypeError(
                "Kafka payload must be bytes or a serializable object"
            )

        packet = serializer()

    await write_packet(
        packet=packet,
        connection_id=connection_id,
    )


# ---------------------------------------------------------------------------
# Redis Kafka queue worker
# ---------------------------------------------------------------------------

async def process_redis_kafka_queue(db: ClientInfo) -> None:
    """
    Publish Redis-queued packets.
    Acknowledge only after Kafka confirms delivery.
    """

    while True:

        record = await db.claim_kafka_packet()

        if record is None:
            await asyncio.sleep(1)
            continue

        try:
            item = json.loads(record)

            packet = base64.b64decode(item["packet"])

            await write_packet(
                packet,
                item["connection_id"],
                item["topic"],
            )

            await db.acknowledge_kafka_packet(record)

        except asyncio.CancelledError:
            await db.requeue_kafka_packet(record)
            raise

        except Exception:
            logger.exception(
                "Queued Kafka packet publish failed; "
                "returning it to pending queue"
            )

            await db.requeue_kafka_packet(record)
            await asyncio.sleep(2)

