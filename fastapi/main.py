import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# Import util.py
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FASTAPI_DIR = Path(__file__).resolve().parent

for import_path in (str(FASTAPI_DIR), str(PROJECT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

import util as utils


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format=(
        "%(asctime)s | %(levelname)-8s | %(name)s | "
        "%(filename)s:%(lineno)d | %(message)s"
    ),
)

logger = logging.getLogger("hv-ves-fastapi")

db = utils.ClientInfo()

redis_monitor_task: asyncio.Task | None = None
kafka_monitor_task: asyncio.Task | None = None

METADATA_REFRESH_SECONDS = int(
    os.getenv("METADATA_REFRESH_SECONDS", "60")
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _request_json(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _connection_not_found_or_unavailable(
    redis_available: bool,
):
    if not redis_available:
        logger.warning(
            "[CONNECTION][LOOKUP_UNAVAILABLE] Redis is unavailable "
            "and no local record exists"
        )
        return JSONResponse(
            status_code=503,
            content={
                "Error": "Connection metadata is temporarily unavailable."
            },
        )

    logger.warning(
        "[CONNECTION][NOT_FOUND] Redis is available but connection "
        "does not exist"
    )
    return JSONResponse(
        status_code=404,
        content={"Error": "Connection id does not exist."},
    )


def _persistence_status(connection_id: str) -> str:
    if connection_id in db.pending_deletes:
        return "pending_delete"
    if connection_id in db.pending_upserts:
        return "pending"
    return "saved"


# ---------------------------------------------------------------------------
# Startup and shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global redis_monitor_task
    global kafka_monitor_task

    startup_time = time.monotonic()

    logger.info("")
    logger.info("=" * 80)
    logger.info("HV-VES INGESTION SERVICE STARTING")
    logger.info("=" * 80)
    logger.info("[STARTUP] Application name : HV-VES Ingestion")
    logger.info("[STARTUP] Process ID        : %s", os.getpid())
    logger.info("[STARTUP] Project directory : %s", PROJECT_ROOT)
    logger.info("[STARTUP] FastAPI directory  : %s", FASTAPI_DIR)
    logger.info("[STARTUP] Log level          : %s", LOG_LEVEL)
    logger.info(
        "[STARTUP] Redis TTL         : %s seconds",
        utils.CONNECTION_TTL if utils.CONNECTION_TTL else "NO EXPIRY",
    )
    logger.info(
        "[STARTUP] Metadata refresh  : %s seconds",
        METADATA_REFRESH_SECONDS,
    )

    # Redis startup check and existing connection loading.
    logger.info("-" * 80)
    logger.info("[STARTUP][REDIS] Checking Redis connectivity")
    redis_ready = await db.init_redis()

    redis_summary = {
        "redis_connections": 0,
        "redis_streams": 0,
        "pending_upserts": 0,
        "pending_deletes": 0,
        "connection_ids": [],
    }

    if redis_ready:
        logger.info(
            "[STARTUP][REDIS] Redis is reachable | Host=%s | Port=%s | DB=%s",
            utils.REDIS_HOST,
            utils.REDIS_PORT,
            utils.REDIS_DB,
        )
        redis_summary = await db.log_existing_connections()
    else:
        logger.error(
            "[STARTUP][REDIS] Redis is unavailable. "
            "Local fallback is enabled; Redis reconnection will be retried."
        )

    # Start background Redis synchronization and monitoring.
    db.start_redis_sync()

    redis_monitor_task = asyncio.create_task(
        db.monitor_redis(),
        name="redis-monitor",
    )

    # Start Kafka connection retry loop.
    logger.info("-" * 80)
    logger.info("[STARTUP][KAFKA] Starting Kafka producer monitor")

    kafka_monitor_task = asyncio.create_task(
        utils.connect_kafka_with_retry(),
        name="kafka-monitor",
    )

    # Consolidated startup report.
    logger.info("")
    logger.info("=" * 80)
    logger.info("HV-VES STARTUP SUMMARY")
    logger.info("=" * 80)
    logger.info(
        "[SUMMARY][REDIS] Status             : %s",
        "CONNECTED" if redis_ready else "UNAVAILABLE",
    )
    logger.info(
        "[SUMMARY][REDIS] Existing connections: %d",
        redis_summary["redis_connections"],
    )
    logger.info(
        "[SUMMARY][REDIS] Existing streams    : %d",
        redis_summary["redis_streams"],
    )
    logger.info(
        "[SUMMARY][REDIS] Connection IDs     : %s",
        redis_summary["connection_ids"],
    )
    logger.info(
        "[SUMMARY][REDIS] Pending local writes: %d",
        len(db.pending_upserts),
    )
    logger.info(
        "[SUMMARY][REDIS] Pending deletions  : %d",
        len(db.pending_deletes),
    )
    logger.info(
        "[SUMMARY][KAFKA] Bootstrap servers  : %s",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "Not configured"),
    )
    logger.info(
        "[SUMMARY][KAFKA] Topic              : %s",
        utils.PERF_KAFKA_TOPIC,
    )
    logger.info(
        "[SUMMARY][KAFKA] Initial status     : %s",
        "CONNECTED" if utils.kafka_producer else "CONNECTING/RETRYING",
    )
    logger.info(
        "[SUMMARY] Startup setup duration : %.2f seconds",
        time.monotonic() - startup_time,
    )
    logger.info("=" * 80)
    logger.info("HV-VES STARTUP INITIALIZATION COMPLETED")
    logger.info("=" * 80)

    try:
        yield

    finally:
        logger.info("")
        logger.info("=" * 80)
        logger.info("[SHUTDOWN] HV-VES service shutdown started")
        logger.info("=" * 80)

        tasks = [
            task
            for task in (redis_monitor_task, kafka_monitor_task)
            if task is not None
        ]

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        redis_monitor_task = None
        kafka_monitor_task = None

        await db.stop_redis_sync()
        await utils.stop_kafka()

        logger.info(
            "[SHUTDOWN] Remaining pending Redis writes=%d deletes=%d",
            len(db.pending_upserts),
            len(db.pending_deletes),
        )
        logger.info("[SHUTDOWN] HV-VES service stopped")


app = FastAPI(
    title="HV-VES Ingestion",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    redis_state = db.redis_status()
    redis_ok = redis_state["connected"]
    kafka_ok = utils.kafka_producer is not None

    logger.info(
        "[HEALTH] Redis=%s | Kafka=%s | PendingWrites=%d | "
        "PendingDeletes=%d",
        "CONNECTED" if redis_ok else "UNAVAILABLE",
        "CONNECTED" if kafka_ok else "UNAVAILABLE",
        len(db.pending_upserts),
        len(db.pending_deletes),
    )

    return JSONResponse(
        status_code=200 if redis_ok else 503,
        content={
            "status": "ok" if redis_ok else "degraded",
            "redis": "connected" if redis_ok else "unavailable",
            "redis_host": redis_state["host"],
            "redis_port": redis_state["port"],
            "redis_db": redis_state["db"],
            "connection_ttl_seconds": utils.CONNECTION_TTL,
            "pending_redis_writes": len(db.pending_upserts),
            "pending_redis_deletes": len(db.pending_deletes),
            "kafka": "connected" if kafka_ok else "unavailable",
            "kafka_topic": utils.PERF_KAFKA_TOPIC,
        },
    )


# ---------------------------------------------------------------------------
# Create a new connection
# ---------------------------------------------------------------------------

@app.post("/connections")
async def create_connection(request: Request):
    body = await _request_json(request)

    logger.info("[CLIENT][REQUEST] New connection request received")

    try:
        parsed = utils.ConnectionRequest.model_validate(body)
    except (ValidationError, ValueError, TypeError) as exc:
        logger.warning(
            "[CLIENT][VALIDATION_FAILED] Connection request rejected: %s",
            exc,
        )
        return JSONResponse(
            status_code=400,
            content={
                "errorReason": "Data does not conform to schema."
            },
        )

    stream_ids = [
        item.stream_id for item in parsed.stream_info_list
    ]

    if len(stream_ids) != len(set(stream_ids)):
        logger.warning(
            "[CLIENT][VALIDATION_FAILED] Duplicate stream IDs | "
            "ProducerID=%s | StreamIDs=%s",
            parsed.producer_id,
            stream_ids,
        )
        return JSONResponse(
            status_code=400,
            content={
                "errorReason": "Duplicate streamId values are not allowed."
            },
        )

    try:
        first = parsed.stream_info_list[0]

        connection = await db.create_connection(
            stream_type=utils.validate_stream_request(first),
            serialization_format=first.serialization_format,
            stream_id=first.stream_id,
            producer_id=parsed.producer_id,
            meas_obj_dn=first.meas_obj_dn,
            performance_metrics=first.performance_metrics,
            vs_data_container=first.vs_data_container,
        )

        for item in parsed.stream_info_list[1:]:
            connection.streams[item.stream_id] = utils.StreamInfo(
                stream_id=item.stream_id,
                stream_type=utils.validate_stream_request(item),
                serialization_format=item.serialization_format,
                meas_obj_dn=item.meas_obj_dn,
                performance_metrics=item.performance_metrics,
                vs_data_container=item.vs_data_container,
            )

        # Save the complete record after all streams have been added.
        redis_saved = await db.save_connection(connection)

    except Exception:
        logger.exception(
            "[CLIENT][CREATE_FAILED] ProducerID=%s",
            parsed.producer_id,
        )
        return JSONResponse(
            status_code=500,
            content={"errorReason": "Unable to create connection."},
        )

    persistence = "saved" if redis_saved else "pending"

    logger.info("=" * 72)
    logger.info("[CLIENT][CONNECTION_CREATED]")
    logger.info("  Producer ID      : %s", connection.producer_id)
    logger.info("  Connection ID    : %s", connection.connection_id)
    logger.info("  Default Stream ID: %s", connection.default_stream_id)
    logger.info("  Total Streams    : %d", len(connection.streams))
    logger.info("  All Stream IDs   : %s", list(connection.streams))
    logger.info("  Redis Persistence: %s", persistence)
    logger.info("=" * 72)

    return JSONResponse(
        status_code=201,
        headers={
            "Location": f"/connections/{connection.connection_id}"
        },
        content={
            "Info": "Connection to hv-ves has been accepted.",
            "connection_id": connection.connection_id,
            "metadata_persistence": persistence,
        },
    )


# ---------------------------------------------------------------------------
# Add a stream to an existing connection
# ---------------------------------------------------------------------------

@app.post("/connections/{connection_id}/streams")
async def create_stream(
    connection_id: str,
    request: Request,
):
    body = await _request_json(request)

    logger.info(
        "[CLIENT][ADD_STREAM_REQUEST] ConnectionID=%s",
        connection_id,
    )

    try:
        parsed = utils.StreamRequest.model_validate(body)
    except (ValidationError, ValueError, TypeError) as exc:
        logger.warning(
            "[STREAM][VALIDATION_FAILED] ConnectionID=%s | Error=%s",
            connection_id, exc,
        )
        return JSONResponse(
            status_code=400,
            content={"Error": "Data does not conform to schema."},
        )

    redis_available, connection = await db.get_connection_lookup(
        connection_id
    )

    if connection is None:
        return _connection_not_found_or_unavailable(redis_available)

    if parsed.stream_id in connection.streams:
        logger.warning(
            "[STREAM][DUPLICATE] ConnectionID=%s | StreamID=%s",
            connection_id, parsed.stream_id,
        )
        return JSONResponse(
            status_code=409,
            content={"Error": "StreamId already exists."},
        )

    try:
        stream = await db.add_stream(
            connection_id=connection_id,
            stream_type=utils.validate_stream_request(parsed),
            serialization_format=parsed.serialization_format,
            stream_id=parsed.stream_id,
            meas_obj_dn=parsed.meas_obj_dn,
            performance_metrics=parsed.performance_metrics,
            vs_data_container=parsed.vs_data_container,
        )
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"Error": str(exc)},
        )
    except Exception:
        logger.exception(
            "[STREAM][ADD_FAILED] ConnectionID=%s",
            connection_id,
        )
        return JSONResponse(
            status_code=500,
            content={"Error": "Unable to add stream."},
        )

    if stream is None:
        return JSONResponse(
            status_code=404,
            content={"Error": "Connection id does not exist."},
        )

    persistence = _persistence_status(connection_id)

    logger.info(
        "[STREAM][ADD_SUCCESS] ProducerID=%s | ConnectionID=%s | "
        "StreamID=%s | TotalStreams=%d | Persistence=%s",
        connection.producer_id,
        connection_id,
        stream.stream_id,
        len(connection.streams),
        persistence,
    )

    return JSONResponse(
        status_code=201,
        content={
            "Info": "Stream successfully added.",
            "connection_id": connection_id,
            "stream_id": stream.stream_id,
            "metadata_persistence": persistence,
        },
    )


# ---------------------------------------------------------------------------
# Get connection information
# ---------------------------------------------------------------------------

@app.get("/connections/{connection_id}")
async def get_connection(connection_id: str):
    logger.info(
        "[CLIENT][GET_CONNECTION] ConnectionID=%s",
        connection_id,
    )

    redis_available, connection = await db.get_connection_lookup(
        connection_id
    )

    if connection is None:
        return _connection_not_found_or_unavailable(redis_available)

    logger.info(
        "[CONNECTION][DETAILS] ProducerID=%s | ConnectionID=%s | "
        "DefaultStreamID=%s | StreamCount=%d | StreamIDs=%s",
        connection.producer_id,
        connection.connection_id,
        connection.default_stream_id,
        len(connection.streams),
        list(connection.streams),
    )

    return JSONResponse(
        status_code=200,
        content={
            "connection_id": connection.connection_id,
            "producerId": connection.producer_id,
            "default_stream_id": connection.default_stream_id,
            "info": {
                sid: utils.stream_response_fields(stream)
                for sid, stream in connection.streams.items()
            },
            "metadata_persistence": _persistence_status(connection_id),
        },
    )


# ---------------------------------------------------------------------------
# Delete a stream
# ---------------------------------------------------------------------------

@app.delete("/connections/{connection_id}/streams/{stream_id}")
async def delete_stream(connection_id: str, stream_id: str):
    logger.info(
        "[CLIENT][DELETE_STREAM_REQUEST] ConnectionID=%s | StreamID=%s",
        connection_id, stream_id,
    )

    redis_available, connection = await db.get_connection_lookup(
        connection_id
    )

    if connection is None:
        return _connection_not_found_or_unavailable(redis_available)

    if stream_id not in connection.streams:
        return JSONResponse(
            status_code=404,
            content={"Error": "Stream id does not exist."},
        )

    if connection.default_stream_id == stream_id:
        logger.warning(
            "[STREAM][DELETE_REJECTED] Cannot delete default stream | "
            "ConnectionID=%s | StreamID=%s",
            connection_id, stream_id,
        )
        return JSONResponse(
            status_code=400,
            content={
                "Error": (
                    f"Default stream for {connection_id} cannot be deleted."
                )
            },
        )

    deleted = await db.delete_stream(connection_id, stream_id)

    if not deleted:
        return JSONResponse(
            status_code=404,
            content={"Error": "Stream id does not exist."},
        )

    logger.info(
        "[STREAM][DELETE_SUCCESS] ConnectionID=%s | StreamID=%s | "
        "RemainingStreams=%d | Persistence=%s",
        connection_id,
        stream_id,
        len(connection.streams) - 1,
        _persistence_status(connection_id),
    )

    return JSONResponse(
        status_code=200,
        content={
            "Info": f"{stream_id} stream has been deleted successfully.",
            "metadata_persistence": _persistence_status(connection_id),
        },
    )


# ---------------------------------------------------------------------------
# Delete a connection
# ---------------------------------------------------------------------------

@app.delete("/connections/{connection_id}")
async def delete_connection(connection_id: str):
    logger.info(
        "[CLIENT][DELETE_CONNECTION_REQUEST] ConnectionID=%s",
        connection_id,
    )

    redis_available, connection = await db.get_connection_lookup(
        connection_id
    )

    if connection is None:
        return _connection_not_found_or_unavailable(redis_available)

    deleted = await db.delete_connection(connection_id)

    if not deleted:
        return JSONResponse(
            status_code=404,
            content={"Error": "Connection id does not exist."},
        )

    persistence = _persistence_status(connection_id)

    logger.info(
        "[CONNECTION][DELETE_SUCCESS] ProducerID=%s | ConnectionID=%s | "
        "Persistence=%s",
        connection.producer_id,
        connection_id,
        persistence,
    )

    return JSONResponse(
        status_code=200,
        content={
            "Info": "Connection deleted successfully.",
            "connection_id": connection_id,
            "metadata_persistence": (
                "pending" if persistence == "pending_delete" else "deleted"
            ),
        },
    )


# ---------------------------------------------------------------------------
# WebSocket: reconnect with the exact existing connection ID
# ---------------------------------------------------------------------------

@app.websocket("/connections/{connection_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    connection_id: str,
):
    await websocket.accept()

    logger.info("=" * 72)
    logger.info("[WEBSOCKET][ACCEPTED]")
    logger.info("  Connection ID: %s", connection_id)
    logger.info("=" * 72)

    delay = 1.0

    try:
        # Never create a new connection ID in this endpoint.
        while True:
            redis_available, connection = (
                await db.get_connection_lookup(connection_id)
            )

            if connection is not None:
                break

            if redis_available:
                logger.warning(
                    "[WEBSOCKET][UNKNOWN_CONNECTION] ConnectionID=%s",
                    connection_id,
                )
                await websocket.close(
                    code=1008,
                    reason="Connection id does not exist.",
                )
                return

            logger.warning(
                "[WEBSOCKET][WAITING_FOR_REDIS] ConnectionID=%s | "
                "RetryIn=%.1fs",
                connection_id, delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)

        logger.info("=" * 72)
        logger.info("[WEBSOCKET][CONNECTION_RESTORED]")
        logger.info("  Producer ID      : %s", connection.producer_id)
        logger.info("  Connection ID    : %s", connection.connection_id)
        logger.info("  Default Stream ID: %s", connection.default_stream_id)
        logger.info("  Stream Count     : %d", len(connection.streams))
        logger.info("  Stream IDs       : %s", list(connection.streams))
        logger.info("  Metadata Source  : %s", (
            "LOCAL_FALLBACK"
            if connection_id in db.pending_upserts
            else "REDIS"
        ))
        logger.info("=" * 72)

        logger.info(
            "[CLIENT][RECONNECT_SUCCESS] Existing ConnectionID=%s reused",
            connection_id,
        )

        last_refresh = time.monotonic()
        packet_count = 0
        total_packet_bytes = 0

        while True:
            try:
                packet = await asyncio.wait_for(
                    websocket.receive_bytes(),
                    timeout=METADATA_REFRESH_SECONDS,
                )

            except asyncio.TimeoutError:
                logger.info(
                    "[WEBSOCKET][IDLE] No packet received during refresh "
                    "interval | ConnectionID=%s",
                    connection_id,
                )

                await db.update_activity(connection_id)
                last_refresh = time.monotonic()
                continue

            packet_count += 1
            packet_size = len(packet)
            total_packet_bytes += packet_size

            logger.info(
                "[CLIENT][MESSAGE_RECEIVED] ConnectionID=%s | "
                "ProducerID=%s | PacketNumber=%d | PacketSize=%d bytes",
                connection_id,
                connection.producer_id,
                packet_count,
                packet_size,
            )

            if packet_size == 0:
                logger.warning(
                    "[CLIENT][EMPTY_PACKET] ConnectionID=%s",
                    connection_id,
                )
                continue

            try:
                await utils.write_packet(
                    packet=packet,
                    connection_id=connection_id,
                )

                logger.info(
                    "[CLIENT][MESSAGE_PROCESSED] ConnectionID=%s | "
                    "PacketNumber=%d | PacketSize=%d bytes",
                    connection_id,
                    packet_count,
                    packet_size,
                )

            except Exception:
                logger.exception(
                    "[CLIENT][MESSAGE_PROCESSING_FAILED] "
                    "ConnectionID=%s | PacketNumber=%d",
                    connection_id,
                    packet_count,
                )
                # Do not silently claim successful Kafka delivery.
                try:
                    await websocket.send_json({
                        "status": "error",
                        "connection_id": connection_id,
                        "message": "Kafka publish failed for this packet.",
                        "packet_number": packet_count,
                    })
                except Exception:
                    logger.warning(
                        "[WEBSOCKET][ERROR_RESPONSE_FAILED] "
                        "ConnectionID=%s",
                        connection_id,
                    )

            if (
                time.monotonic() - last_refresh
                >= METADATA_REFRESH_SECONDS
            ):
                await db.update_activity(connection_id)
                last_refresh = time.monotonic()

    except WebSocketDisconnect as exc:
        logger.info(
            "[CLIENT][DISCONNECTED] ConnectionID=%s | ProducerID=%s | "
            "PacketsReceived=%d | TotalBytes=%d | DisconnectCode=%s",
            connection_id,
            connection.producer_id if "connection" in locals() else "UNKNOWN",
            packet_count if "packet_count" in locals() else 0,
            total_packet_bytes if "total_packet_bytes" in locals() else 0,
            getattr(exc, "code", 1000),
        )

    except asyncio.CancelledError:
        logger.warning(
            "[WEBSOCKET][CANCELLED] ConnectionID=%s",
            connection_id,
        )
        raise

    except Exception:
        logger.exception(
            "[WEBSOCKET][ERROR] ConnectionID=%s",
            connection_id,
        )
        try:
            await websocket.close(
                code=1011,
                reason="Internal server error",
            )
        except Exception:
            logger.debug(
                "[WEBSOCKET] Socket already closed | ConnectionID=%s",
                connection_id,
            )

    finally:
        logger.info(
            "[WEBSOCKET][CLOSED] ConnectionID=%s",
            connection_id,
        )

