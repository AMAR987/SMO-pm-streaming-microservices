import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, ConfigDict, Field, model_validator
from redis.asyncio import Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

logger = logging.getLogger("hv-ves")

# ---------------------------------------------------------------------------
# Configuration
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

REDIS_RETRY_ERRORS = [
    RedisConnectionError,
    RedisTimeoutError,
    ConnectionResetError,
]

PERF_KAFKA_TOPIC = os.getenv(
    "PERF_KAFKA_TOPIC", "HV_VES_PERF3GPP"
)
KAFKA_CONFIG_FILE = os.getenv("KAFKA_CONFIG_FILE", "meta.json")


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------

class StreamInfoRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    stream_id: str = Field(..., alias="streamId", min_length=1)
    stream_type: str = Field(..., alias="StreamType", min_length=1)
    serialization_format: str = Field(
        "GPB", alias="SerializationFormat"
    )
    meas_obj_dn: str = Field("", alias="measObjDn")
    performance_metrics: List[Any] = Field(
        default_factory=list, alias="performanceMetrics"
    )
    vs_data_container: Dict[str, Any] = Field(
        default_factory=dict, alias="VsDataContainer"
    )

    @model_validator(mode="before")
    @classmethod
    def unwrap_stream(cls, values):
        if isinstance(values, dict) and "streamInfoList" in values:
            wrapped = values["streamInfoList"]
            if isinstance(wrapped, list):
                if len(wrapped) != 1:
                    raise ValueError(
                        "streamInfoList must contain exactly one stream"
                    )
                wrapped = wrapped[0]
            if not isinstance(wrapped, dict):
                raise ValueError("streamInfoList must be an object")
            return wrapped
        return values

    @model_validator(mode="after")
    def validate_stream(self):
        if self.stream_type.strip().upper() not in {
            "PERFORMANCE", "PERF3GPP"
        }:
            raise ValueError(
                "Only PERFORMANCE or PERF3GPP StreamType is supported"
            )
        if self.serialization_format.strip().upper() != "GPB":
            raise ValueError("Only GPB SerializationFormat is supported")
        self.stream_type = "PERFORMANCE"
        self.serialization_format = "GPB"
        return self


class ConnectionRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    producer_id: str = Field(..., alias="producerId", min_length=1)
    stream_info_list: List[StreamInfoRequest] = Field(
        ..., alias="streamInfoList", min_length=1
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_streams(cls, values):
        if isinstance(values, dict):
            streams = values.get(
                "streamInfoList", values.get("stream_info_list")
            )
            if isinstance(streams, dict):
                values = dict(values)
                values["streamInfoList"] = [streams]
        return values


class StreamRequest(StreamInfoRequest):
    pass


# ---------------------------------------------------------------------------
# Metadata models
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
# Redis client and connection metadata manager
# ---------------------------------------------------------------------------

class ClientInfo:
    def __init__(self):
        self.redis: Optional[Redis] = None
        self.redis_init_lock = asyncio.Lock()
        self.redis_sync_task: Optional[asyncio.Task] = None
        self.pending_upserts: Dict[str, ConnectionInfo] = {}
        self.pending_deletes: set[str] = set()

    def _redis_key(self, connection_id: str) -> str:
        return f"{REDIS_KEY_PREFIX}{connection_id}"

    async def init_redis(self) -> bool:
        async with self.redis_init_lock:
            if self.redis is not None:
                try:
                    await self.redis.ping()
                    return True
                except Exception:
                    logger.exception("[REDIS][HEALTH] Existing Redis connection failed")
                    await self.close_redis()

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
                socket_connect_timeout=2,
                socket_timeout=5,
            )

            try:
                started = time.monotonic()
                await candidate.ping()
                self.redis = candidate
                logger.info(
                    "[REDIS][CONNECTED] Ping successful | Host=%s | "
                    "Port=%s | DB=%s | ResponseTimeMs=%.2f",
                    REDIS_HOST,
                    REDIS_PORT,
                    REDIS_DB,
                    (time.monotonic() - started) * 1000,
                )
                return True
            except Exception:
                logger.exception(
                    "[REDIS][UNAVAILABLE] Could not connect | Host=%s | "
                    "Port=%s | DB=%s",
                    REDIS_HOST, REDIS_PORT, REDIS_DB,
                )
                try:
                    await candidate.aclose()
                except Exception:
                    logger.exception("[REDIS] Candidate close failed")
                return False

    async def close_redis(self):
        client = self.redis
        self.redis = None
        if client is not None:
            try:
                await client.aclose()
                logger.info("[REDIS][CLOSED] Redis client closed")
            except Exception:
                logger.exception("[REDIS][CLOSE_ERROR]")

    def redis_status(self) -> dict:
        return {
            "connected": self.redis is not None,
            "host": REDIS_HOST,
            "port": REDIS_PORT,
            "db": REDIS_DB,
            "pending_upserts": len(self.pending_upserts),
            "pending_deletes": len(self.pending_deletes),
        }

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
        for item in data.get("streams", []):
            stream = StreamInfo(
                stream_id=item["stream_id"],
                stream_type=item.get("stream_type", "PERFORMANCE"),
                serialization_format=item.get(
                    "serialization_format", "GPB"
                ),
                meas_obj_dn=item.get("meas_obj_dn", ""),
                performance_metrics=item.get("performance_metrics", []),
                vs_data_container=item.get("vs_data_container", {}),
                last_activity=item.get("last_activity", time.time()),
            )
            connection.streams[stream.stream_id] = stream

        if connection.default_stream_id is None and connection.streams:
            connection.default_stream_id = next(iter(connection.streams))
        return connection

    async def _redis_set(self, connection: ConnectionInfo) -> bool:
        try:
            payload = json.dumps(
                self._connection_to_dict(connection),
                separators=(",", ":"),
            )
        except (TypeError, ValueError):
            logger.exception(
                "[REDIS][SERIALIZE_ERROR] ConnectionID=%s",
                connection.connection_id,
            )
            return False

        if not await self.init_redis():
            return False

        key = self._redis_key(connection.connection_id)
        try:
            if CONNECTION_TTL > 0:
                await self.redis.set(key, payload, ex=CONNECTION_TTL)
            else:
                await self.redis.set(key, payload)

            logger.info(
                "[REDIS][WRITE_SUCCESS] ConnectionID=%s | Key=%s | "
                "TTL=%s | ProducerID=%s | StreamCount=%d",
                connection.connection_id,
                key,
                CONNECTION_TTL if CONNECTION_TTL else "NO_EXPIRY",
                connection.producer_id,
                len(connection.streams),
            )
            return True
        except Exception:
            logger.exception(
                "[REDIS][WRITE_FAILED] ConnectionID=%s",
                connection.connection_id,
            )
            await self.close_redis()
            return False

    async def _redis_get(
        self, connection_id: str
    ) -> tuple[bool, Optional[ConnectionInfo]]:
        if not await self.init_redis():
            logger.warning(
                "[REDIS][READ_UNAVAILABLE] ConnectionID=%s",
                connection_id,
            )
            return False, None

        try:
            key = self._redis_key(connection_id)
            raw = await self.redis.get(key)

            if raw is None:
                logger.info(
                    "[REDIS][READ_NOT_FOUND] ConnectionID=%s | Key=%s",
                    connection_id, key,
                )
                return True, None

            connection = self._dict_to_connection(json.loads(raw))
            logger.info(
                "[REDIS][READ_SUCCESS] ConnectionID=%s | ProducerID=%s | "
                "StreamCount=%d",
                connection_id,
                connection.producer_id,
                len(connection.streams),
            )
            return True, connection

        except Exception:
            logger.exception(
                "[REDIS][READ_FAILED] ConnectionID=%s", connection_id
            )
            await self.close_redis()
            return False, None

    async def _redis_delete(self, connection_id: str) -> bool:
        if not await self.init_redis():
            return False
        try:
            key = self._redis_key(connection_id)
            await self.redis.delete(key)
            logger.info(
                "[REDIS][DELETE_SUCCESS] ConnectionID=%s | Key=%s",
                connection_id, key,
            )
            return True
        except Exception:
            logger.exception(
                "[REDIS][DELETE_FAILED] ConnectionID=%s",
                connection_id,
            )
            await self.close_redis()
            return False

    def _store_pending_upsert(self, connection: ConnectionInfo):
        cid = connection.connection_id
        self.pending_deletes.discard(cid)
        self.pending_upserts[cid] = connection
        logger.warning(
            "[REDIS][LOCAL_FALLBACK][WRITE] ConnectionID=%s | "
            "ProducerID=%s | StreamIDs=%s | Reason=Redis unavailable",
            cid,
            connection.producer_id,
            list(connection.streams),
        )

    def _store_pending_delete(self, connection_id: str):
        self.pending_upserts.pop(connection_id, None)
        self.pending_deletes.add(connection_id)
        logger.warning(
            "[REDIS][LOCAL_FALLBACK][DELETE] ConnectionID=%s | "
            "Delete queued until Redis recovers",
            connection_id,
        )

    async def save_connection(self, connection: ConnectionInfo) -> bool:
        if await self._redis_set(connection):
            self.pending_upserts.pop(connection.connection_id, None)
            self.pending_deletes.discard(connection.connection_id)
            return True

        self._store_pending_upsert(connection)
        return False

    async def get_connection_lookup(
        self, connection_id: str
    ) -> tuple[bool, Optional[ConnectionInfo]]:
        if connection_id in self.pending_deletes:
            logger.warning(
                "[CONNECTION][LOOKUP] Connection is pending deletion | "
                "ConnectionID=%s",
                connection_id,
            )
            return self.redis is not None, None

        if connection_id in self.pending_upserts:
            logger.info(
                "[CONNECTION][LOOKUP] Using local fallback record | "
                "ConnectionID=%s",
                connection_id,
            )
            return self.redis is not None, self.pending_upserts[connection_id]

        return await self._redis_get(connection_id)

    async def get_connection(
        self, connection_id: str
    ) -> Optional[ConnectionInfo]:
        _, connection = await self.get_connection_lookup(connection_id)
        return connection

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
        stream_id = stream_id or str(uuid.uuid4())
        connection = ConnectionInfo(
            connection_id=uuid.uuid4().hex[:15],
            producer_id=producer_id,
            default_stream_id=stream_id,
        )
        connection.streams[stream_id] = StreamInfo(
            stream_id=stream_id,
            meas_obj_dn=meas_obj_dn,
            performance_metrics=list(performance_metrics or []),
            vs_data_container=dict(vs_data_container or {}),
        )
        logger.info(
            "[CONNECTION][CREATE_BEGIN] ConnectionID=%s | ProducerID=%s | "
            "DefaultStreamID=%s",
            connection.connection_id, producer_id, stream_id,
        )
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
        if not stream_id:
            raise ValueError("stream_id is required")

        connection = await self.get_connection(connection_id)
        if connection is None:
            return None
        if stream_id in connection.streams:
            raise ValueError(f"Duplicate streamId: {stream_id}")

        stream = StreamInfo(
            stream_id=stream_id,
            meas_obj_dn=meas_obj_dn,
            performance_metrics=list(performance_metrics or []),
            vs_data_container=dict(vs_data_container or {}),
        )
        connection.streams[stream_id] = stream
        connection.last_activity = time.time()

        logger.info(
            "[STREAM][ADD_BEGIN] ConnectionID=%s | ProducerID=%s | "
            "StreamID=%s | TotalStreams=%d",
            connection_id,
            connection.producer_id,
            stream_id,
            len(connection.streams),
        )
        await self.save_connection(connection)
        return stream

    async def update_activity(
        self, connection_id: str, stream_id: Optional[str] = None
    ) -> bool:
        connection = await self.get_connection(connection_id)
        if connection is None:
            logger.warning(
                "[CONNECTION][ACTIVITY] Record unavailable | ConnectionID=%s",
                connection_id,
            )
            return False

        now = time.time()
        connection.last_activity = now
        if stream_id and stream_id in connection.streams:
            connection.streams[stream_id].last_activity = now

        saved = await self.save_connection(connection)
        logger.info(
            "[CONNECTION][ACTIVITY] ConnectionID=%s | Persistence=%s",
            connection_id,
            "REDIS" if saved else "LOCAL_PENDING",
        )
        return saved

    async def delete_stream(
        self, connection_id: str, stream_id: str
    ) -> bool:
        connection = await self.get_connection(connection_id)
        if connection is None or stream_id not in connection.streams:
            return False
        if connection.default_stream_id == stream_id:
            return False

        del connection.streams[stream_id]
        connection.last_activity = time.time()
        await self.save_connection(connection)

        logger.info(
            "[STREAM][DELETE_SUCCESS] ConnectionID=%s | StreamID=%s | "
            "RemainingStreams=%d",
            connection_id, stream_id, len(connection.streams),
        )
        return True

    async def delete_connection(self, connection_id: str) -> bool:
        _, connection = await self.get_connection_lookup(connection_id)
        if connection is None:
            return False

        if await self._redis_delete(connection_id):
            self.pending_upserts.pop(connection_id, None)
            self.pending_deletes.discard(connection_id)
        else:
            self._store_pending_delete(connection_id)

        logger.info(
            "[CONNECTION][DELETE] ConnectionID=%s | ProducerID=%s",
            connection_id, connection.producer_id,
        )
        return True

    async def log_existing_connections(self) -> dict:
        summary = {
            "redis_connections": 0,
            "redis_streams": 0,
            "pending_upserts": 0,
            "pending_deletes": 0,
            "connection_ids": [],
        }

        logger.info("=" * 72)
        logger.info("[STARTUP][REDIS] Loading existing connection information")
        logger.info("=" * 72)

        if not await self.init_redis():
            logger.warning(
                "[STARTUP][REDIS] Cannot scan Redis: service unavailable"
            )
            summary["pending_upserts"] = len(self.pending_upserts)
            summary["pending_deletes"] = len(self.pending_deletes)
            return summary

        try:
            async for key in self.redis.scan_iter(
                match=f"{REDIS_KEY_PREFIX}*"
            ):
                raw = await self.redis.get(key)
                if not raw:
                    continue

                try:
                    connection = self._dict_to_connection(json.loads(raw))
                except Exception:
                    logger.exception(
                        "[STARTUP][REDIS] Invalid record | Key=%s", key
                    )
                    continue

                ttl = await self.redis.ttl(key)
                summary["redis_connections"] += 1
                summary["redis_streams"] += len(connection.streams)
                summary["connection_ids"].append(
                    connection.connection_id
                )

                logger.info("-" * 72)
                logger.info("[CONNECTION][EXISTING] Redis record loaded")
                logger.info("  Connection ID : %s", connection.connection_id)
                logger.info("  Producer ID   : %s", connection.producer_id)
                logger.info(
                    "  Default Stream: %s", connection.default_stream_id
                )
                logger.info("  Stream Count  : %d", len(connection.streams))
                logger.info("  Stream IDs    : %s", list(connection.streams))
                logger.info("  Redis Key     : %s", key)
                logger.info("  Redis TTL     : %s seconds", ttl)
                logger.info(
                    "  Created At    : %s",
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(connection.created_at),
                    ),
                )
                logger.info(
                    "  Last Activity : %s",
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(connection.last_activity),
                    ),
                )

                for index, stream in enumerate(
                    connection.streams.values(), start=1
                ):
                    logger.info(
                        "  [STREAM %d] ID=%s | Type=%s | Format=%s | "
                        "MeasObjDN=%s",
                        index,
                        stream.stream_id,
                        stream.stream_type,
                        stream.serialization_format,
                        stream.meas_obj_dn,
                    )

            logger.info("-" * 72)
            logger.info(
                "[STARTUP][REDIS] Redis scan completed | "
                "Connections=%d | Streams=%d",
                summary["redis_connections"],
                summary["redis_streams"],
            )

        except Exception:
            logger.exception("[STARTUP][REDIS] Failed while scanning records")

        summary["pending_upserts"] = len(self.pending_upserts)
        summary["pending_deletes"] = len(self.pending_deletes)

        for cid, connection in self.pending_upserts.items():
            logger.warning(
                "[STARTUP][LOCAL_PENDING] ConnectionID=%s | ProducerID=%s | "
                "StreamIDs=%s",
                cid, connection.producer_id, list(connection.streams),
            )

        return summary

    async def sync_pending_to_redis(self):
        if not await self.init_redis():
            return

        for cid in list(self.pending_deletes):
            logger.info(
                "[REDIS][SYNC][DELETE_BEGIN] ConnectionID=%s", cid
            )
            if not await self._redis_delete(cid):
                return
            self.pending_deletes.discard(cid)
            logger.info(
                "[REDIS][SYNC][DELETE_COMPLETE] ConnectionID=%s", cid
            )

        for cid, connection in list(self.pending_upserts.items()):
            logger.info(
                "[REDIS][SYNC][WRITE_BEGIN] ConnectionID=%s | "
                "ProducerID=%s | Streams=%s",
                cid, connection.producer_id, list(connection.streams),
            )
            if not await self._redis_set(connection):
                return

            if self.pending_upserts.get(cid) is connection:
                self.pending_upserts.pop(cid, None)

            logger.info(
                "[REDIS][SYNC][WRITE_COMPLETE] ConnectionID=%s", cid
            )

    async def monitor_redis(self):
        delay = REDIS_BACKOFF_BASE
        while True:
            try:
                if await self.init_redis():
                    logger.debug("[REDIS][MONITOR] Redis is healthy")
                    delay = REDIS_BACKOFF_BASE
                    await self.sync_pending_to_redis()
                    await asyncio.sleep(REDIS_SYNC_INTERVAL)
                else:
                    logger.warning(
                        "[REDIS][MONITOR] Redis unavailable; retry in %.1fs",
                        delay,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, REDIS_BACKOFF_CAP)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[REDIS][MONITOR] Unexpected monitor error")
                await asyncio.sleep(delay)
                delay = min(delay * 2, REDIS_BACKOFF_CAP)

    async def _redis_sync_loop(self):
        while True:
            try:
                await self.sync_pending_to_redis()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[REDIS][SYNC] Synchronization loop error")
            await asyncio.sleep(REDIS_SYNC_INTERVAL)

    def start_redis_sync(self):
        if self.redis_sync_task is None or self.redis_sync_task.done():
            self.redis_sync_task = asyncio.create_task(
                self._redis_sync_loop(),
                name="redis-sync",
            )
            logger.info("[REDIS][SYNC] Background sync task started")

    async def stop_redis_sync(self):
        if self.redis_sync_task:
            self.redis_sync_task.cancel()
            try:
                await self.redis_sync_task
            except asyncio.CancelledError:
                pass
            self.redis_sync_task = None

        # Final best-effort synchronization before shutdown.
        await self.sync_pending_to_redis()
        await self.close_redis()


# ---------------------------------------------------------------------------
# Helpers used by main.py
# ---------------------------------------------------------------------------

def validate_stream_request(request: StreamInfoRequest) -> str:
    return "PERFORMANCE"


def stream_response_fields(stream: StreamInfo) -> dict:
    return {
        "StreamType": stream.stream_type,
        "performanceMetrics": stream.performance_metrics,
    }


def connection_response(connection: ConnectionInfo) -> dict:
    return {
        "connection_id": connection.connection_id,
        "producerId": connection.producer_id,
        "info": {
            sid: stream_response_fields(stream)
            for sid, stream in connection.streams.items()
        },
    }


def stream_response(connection_id: str, stream: StreamInfo) -> dict:
    return {
        "connection_id": connection_id,
        "stream_id": stream.stream_id,
        "stream_type": stream.stream_type,
        "serialization_format": stream.serialization_format,
    }


def allocate_connection_id() -> str:
    return uuid.uuid4().hex[:15]


def validate_data(data: dict) -> bool:
    try:
        parsed = ConnectionRequest.model_validate(data)
        ids = [stream.stream_id for stream in parsed.stream_info_list]
        return len(ids) == len(set(ids))
    except Exception:
        return False


STATUS_BAD_REQUEST = 400
STATUS_OK = 200
STATUS_ACCEPTED = 201
STATUS_SERVER_ERROR = 503
STATUS_FORBIDDEN = 403
STATUS_SWITCH_PROTOCOL = 101


# ---------------------------------------------------------------------------
# Kafka configuration and producer
# ---------------------------------------------------------------------------

kafka_bootstrap_server = ""
kafka_user = ""
kafka_password = ""
kafka_security_protocol = os.getenv(
    "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"
)
kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

kafka_producer: Optional[AIOKafkaProducer] = None
kafka_start_lock = asyncio.Lock()


def get_kafka_configs():
    global kafka_bootstrap_server, kafka_user, kafka_password
    global kafka_security_protocol, kafka_sasl_mechanism

    kafka_bootstrap_server = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", ""
    ).strip()
    kafka_user = os.getenv("KAFKA_USERNAME", "")
    kafka_password = os.getenv("KAFKA_PASSWORD", "")
    kafka_security_protocol = os.getenv(
        "KAFKA_SECURITY_PROTOCOL", kafka_security_protocol
    )
    kafka_sasl_mechanism = os.getenv(
        "KAFKA_SASL_MECHANISM", kafka_sasl_mechanism
    )

    config_path = Path(KAFKA_CONFIG_FILE)
    if not config_path.is_absolute() and not config_path.exists():
        for candidate in (
            Path(__file__).resolve().parent / config_path,
            Path(__file__).resolve().parent.parent / config_path,
            Path(__file__).resolve().parent.parent.parent / config_path,
        ):
            if candidate.exists():
                config_path = candidate
                break

    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as file:
            config = json.load(file).get("kafka", {})

        kafka_bootstrap_server = (
            kafka_bootstrap_server or config.get("bootstrap_server", "")
        )
        kafka_user = kafka_user or config.get("username", "")
        kafka_password = kafka_password or config.get("password", "")
        kafka_security_protocol = os.getenv(
            "KAFKA_SECURITY_PROTOCOL",
            config.get("security_protocol", kafka_security_protocol),
        )
        kafka_sasl_mechanism = os.getenv(
            "KAFKA_SASL_MECHANISM",
            config.get("sasl_mechanism", kafka_sasl_mechanism),
        )

    if not kafka_bootstrap_server:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS is unset and no bootstrap_server "
            "was found in meta.json"
        )

    logger.info(
        "[KAFKA][CONFIG] Bootstrap=%s | Protocol=%s | Topic=%s",
        kafka_bootstrap_server,
        kafka_security_protocol,
        PERF_KAFKA_TOPIC,
    )


async def start_kafka():
    global kafka_producer

    async with kafka_start_lock:
        if kafka_producer is not None:
            return

        get_kafka_configs()

        options = {
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
                    "KAFKA_USERNAME and KAFKA_PASSWORD are required for SASL"
                )
            options.update({
                "sasl_mechanism": kafka_sasl_mechanism,
                "sasl_plain_username": kafka_user,
                "sasl_plain_password": kafka_password,
            })

        producer = AIOKafkaProducer(**options)

        try:
            await producer.start()
            kafka_producer = producer
            logger.info(
                "[KAFKA][CONNECTED] Producer started | Bootstrap=%s | "
                "Topic=%s",
                kafka_bootstrap_server,
                PERF_KAFKA_TOPIC,
            )
        except Exception:
            try:
                await producer.stop()
            except Exception:
                logger.exception("[KAFKA] Failed to close producer")
            raise


async def connect_kafka_with_retry():
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
                        "[KAFKA][CONNECT_FAILED] Retry in %.1fs", delay
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, cap)
                    continue
            await asyncio.sleep(1)

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[KAFKA][MONITOR] Unexpected error")
            await asyncio.sleep(delay)
            delay = min(delay * 2, cap)


async def stop_kafka():
    global kafka_producer
    producer = kafka_producer
    kafka_producer = None

    if producer is not None:
        try:
            await producer.stop()
            logger.info("[KAFKA][STOPPED] Producer stopped")
        except Exception:
            logger.exception("[KAFKA][STOP_ERROR]")


async def write_packet(packet: bytes, connection_id: str):
    global kafka_producer

    if not isinstance(packet, bytes):
        raise TypeError("Kafka packet must be bytes")
    if not connection_id:
        raise ValueError("connection_id is required")

    retries = int(os.getenv("KAFKA_SEND_RETRY_COUNT", "5"))
    delay = float(os.getenv("KAFKA_SEND_RETRY_BASE", "0.5"))
    cap = float(os.getenv("KAFKA_SEND_RETRY_CAP", "10"))

    logger.info(
        "[KAFKA][SEND_BEGIN] ConnectionID=%s | Topic=%s | PacketBytes=%d",
        connection_id, PERF_KAFKA_TOPIC, len(packet),
    )

    for attempt in range(1, retries + 1):
        if kafka_producer is None:
            try:
                await start_kafka()
            except Exception:
                logger.exception(
                    "[KAFKA][START_FAILED] Attempt=%d/%d",
                    attempt, retries,
                )

        producer = kafka_producer
        if producer is not None:
            try:
                metadata = await producer.send_and_wait(
                    topic=PERF_KAFKA_TOPIC,
                    key=connection_id.encode("utf-8"),
                    value=packet,
                )
                logger.info(
                    "[KAFKA][PUBLISH_SUCCESS] ConnectionID=%s | "
                    "Topic=%s | Partition=%s | Offset=%s | Bytes=%d",
                    connection_id,
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                    len(packet),
                )
                return

            except Exception:
                logger.exception(
                    "[KAFKA][PUBLISH_FAILED] ConnectionID=%s | "
                    "Attempt=%d/%d",
                    connection_id, attempt, retries,
                )
                if kafka_producer is producer:
                    await stop_kafka()

        if attempt < retries:
            logger.warning(
                "[KAFKA][RETRY] ConnectionID=%s | Attempt=%d/%d | "
                "RetryIn=%.1fs",
                connection_id, attempt, retries, delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, cap)

    logger.error(
        "[KAFKA][SEND_EXHAUSTED] ConnectionID=%s | Attempts=%d",
        connection_id, retries,
    )
    raise RuntimeError(
        f"Kafka unavailable after {retries} attempts "
        f"for topic={PERF_KAFKA_TOPIC}"
    )


async def init_kafka():
    await start_kafka()


async def close_kafka():
    await stop_kafka()


async def async_kafka_send(data, connection_id: str):
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

    await write_packet(packet, connection_id)

