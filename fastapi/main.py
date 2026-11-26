import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
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

try:
    import util as utils
except ImportError as exc:
    raise ImportError(
        "Unable to import util.py from the FastAPI project directory"
    ) from exc


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
kafka_queue_task: asyncio.Task | None = None

app = FastAPI(
    title="HV-VES Ingestion",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

async def _request_json(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}

    return body if isinstance(body, dict) else {}


def _validate(model, body):
    """Support Pydantic v1 and v2."""

    if hasattr(model, "model_validate"):
        return model.model_validate(body)

    return model.parse_obj(body)


def _connection_not_found_or_unavailable(
    redis_available: bool,
):
    if not redis_available:
        logger.warning(
            "[CONNECTION][LOOKUP_UNAVAILABLE] "
            "Redis is unavailable and no local record exists"
        )

        return JSONResponse(
            status_code=503,
            content={
                "Error": "Connection metadata is temporarily unavailable."
            },
        )

    logger.warning(
        "[CONNECTION][NOT_FOUND] "
        "Redis is available but connection does not exist"
    )

    return JSONResponse(
        status_code=404,
        content={
            "Error": "Connection id does not exist."
        },
    )


# ---------------------------------------------------------------------------
# Startup event - replaces lifespan startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():

    global redis_monitor_task
    global kafka_monitor_task
    global kafka_queue_task
    logger.info("=" * 72)
    logger.info("[STARTUP] HV-VES ingestion service starting")
    logger.info("[STARTUP] Process ID: %s", os.getpid())
    logger.info("=" * 72)

    redis_ready = await db.init_redis()

    if redis_ready:
        logger.info(
            "[STARTUP][REDIS] Redis ready; "
            "loading existing connection inventory"
        )

        await db.log_existing_connections()

    else:
        logger.warning(
            "[STARTUP][REDIS] Redis unavailable; "
            "background monitoring will retry"
        )
    await db.recover_kafka_queue()
    redis_monitor_task = asyncio.create_task(
        db.monitor_redis(),
        name="redis-monitor",
    )
    kafka_monitor_task = asyncio.create_task(
        utils.connect_kafka_with_retry(),
        name="kafka-monitor",
    )

    kafka_queue_task = asyncio.create_task(
        utils.process_redis_kafka_queue(db),
        name="kafka-queue-worker",
    )

    logger.info(
        "[STARTUP] Redis monitor, Kafka monitor "
        "and Kafka queue worker started"
    )

    logger.info(
        "[STARTUP] HV-VES service ready"
    )


# ---------------------------------------------------------------------------
# Shutdown event - replaces lifespan shutdown
# ---------------------------------------------------------------------------

@app.on_event("shutdown")
async def shutdown_event():

    global redis_monitor_task
    global kafka_monitor_task
    global kafka_queue_task

    logger.info(
        "[SHUTDOWN] HV-VES ingestion service stopping"
    )

    tasks = [
        redis_monitor_task,
        kafka_monitor_task,
        kafka_queue_task,
    ]

    active_tasks = [
        task for task in tasks
        if task is not None
    ]

    for task in active_tasks:
        if not task.done():
            task.cancel()

    if active_tasks:
        await asyncio.gather(
            *active_tasks,
            return_exceptions=True,
        )

    redis_monitor_task = None
    kafka_monitor_task = None
    kafka_queue_task = None

    await db.stop_redis_sync()
    await utils.stop_kafka()

    logger.info(
        "[SHUTDOWN] HV-VES ingestion service stopped"
    )


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():

    redis_state = await db.redis_status()
    redis_ok = redis_state["connected"]

    return JSONResponse(
        status_code=200 if redis_ok else 503,
        content={
            "status": "ok" if redis_ok else "degraded",
            "redis": (
                "connected" if redis_ok else "unavailable"
            ),
            "redis_db": redis_state["db"],
            "connection_ttl_seconds": utils.CONNECTION_TTL,
            "pending_redis_writes": len(db.pending_upserts),
        },
    )


# ---------------------------------------------------------------------------
# Create connection
# ---------------------------------------------------------------------------

@app.post("/connections")
async def create_connection(request: Request):

    body = await _request_json(request)

    try:
        parsed = _validate(
            utils.ConnectionRequest,
            body,
        )

    except (ValidationError, ValueError, TypeError) as exc:

        logger.info(
            "Connection request rejected: %s",
            exc,
        )

        message = (
            "Either no data was provided or the data sent has error."
            if not body
            else "Data does not conform to schema"
        )

        return JSONResponse(
            status_code=400,
            content={
                "errorReason": message
            },
        )

    first = parsed.stream_info_list[0]

    stream_ids = [
        item.stream_id
        for item in parsed.stream_info_list
    ]

    if len(stream_ids) != len(set(stream_ids)):
        return JSONResponse(
            status_code=400,
            content={
                "errorReason": "Duplicate streamId in streamInfoList"
            },
        )

    try:

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

            if item.stream_id in connection.streams:
                raise ValueError(
                    f"Duplicate streamId: {item.stream_id}"
                )

            connection.streams[item.stream_id] = utils.StreamInfo(
                stream_id=item.stream_id,
                stream_type=utils.validate_stream_request(item),
                serialization_format=item.serialization_format,
                meas_obj_dn=item.meas_obj_dn,
                performance_metrics=item.performance_metrics,
                vs_data_container=item.vs_data_container,
            )
        if (
            len(parsed.stream_info_list) > 1
            and not await db.save_connection(connection)
        ):

            db.pending_upserts.pop(
                connection.connection_id,
                None,
            )

            logger.error(
                "Could not persist all streams for connection %s",
                connection.connection_id,
            )

            return JSONResponse(
                status_code=503,
                content={
                    "errorReason": (
                        "Redis metadata persistence failed; "
                        "retry the request."
                    )
                },
            )

    except utils.RedisPersistenceError as exc:

        logger.error(
            "Connection creation failed: %s",
            exc,
        )

        return JSONResponse(
            status_code=503,
            content={
                "errorReason": (
                    "Redis is unavailable; connection was not created. "
                    "Retry the request."
                )
            },
        )

    except (ValidationError, ValueError, TypeError) as exc:

        logger.info(
            "Connection request rejected: %s",
            exc,
        )

        return JSONResponse(
            status_code=400,
            content={
                "errorReason": "Data does not conform to schema"
            },
        )

    logger.info(
        "Connection created | ConnectionID=%s | DefaultStreamID=%s",
        connection.connection_id,
        first.stream_id,
    )

    return JSONResponse(
        status_code=201,
        headers={
            "Location": f"/connections/{connection.connection_id}"
        },
        content={
            "Info": "Connection to hv-ves has been accepted.",
            "connection_id": connection.connection_id,
        },
    )


# ---------------------------------------------------------------------------
# Create stream
# ---------------------------------------------------------------------------

@app.post("/connections/{connection_id}/streams")
async def create_stream(
    connection_id: str,
    request: Request,
):

    body = await _request_json(request)

    try:
        parsed = _validate(
            utils.StreamRequest,
            body,
        )

    except (ValidationError, ValueError, TypeError) as exc:

        logger.info(
            "Stream request rejected: %s",
            exc,
        )

        return JSONResponse(
            status_code=400,
            content={
                "Error": "Data does not conform to schema."
            },
        )

    redis_available, connection = (
        await db.get_connection_lookup(connection_id)
    )

    if connection is None:
        return _connection_not_found_or_unavailable(
            redis_available
        )

    if parsed.stream_id in connection.streams:
        return JSONResponse(
            status_code=400,
            content={
                "Error": (
                    "New stream cannot be created; "
                    "streamId already exists."
                )
            },
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

    except utils.RedisPersistenceError as exc:

        logger.error(
            "Stream creation failed for %s: %s",
            connection_id,
            exc,
        )

        return JSONResponse(
            status_code=503,
            content={
                "Error": (
                    "Redis metadata persistence failed; "
                    "retry the request."
                )
            },
        )

    if stream is None:
        return JSONResponse(
            status_code=404,
            content={
                "Error": "Connection id does not exist."
            },
        )

    logger.info(
        "Stream added | ConnectionID=%s | StreamID=%s",
        connection_id,
        stream.stream_id,
    )

    return JSONResponse(
        status_code=200,
        content={
            "Info": "Stream successfully added."
        },
    )


# ---------------------------------------------------------------------------
# Get connection
# ---------------------------------------------------------------------------

@app.get("/connections/{connection_id}")
async def get_connection(connection_id: str):

    redis_available, connection = (
        await db.get_connection_lookup(connection_id)
    )

    if connection is None:
        return _connection_not_found_or_unavailable(
            redis_available
        )

    info = {
        sid: utils.stream_response_fields(stream)
        for sid, stream in connection.streams.items()
    }

    return JSONResponse(
        status_code=200,
        content={
            "info": info
        },
    )


# ---------------------------------------------------------------------------
# Delete stream
# ---------------------------------------------------------------------------

@app.delete(
    "/connections/{connection_id}/streams/{stream_id}"
)
async def delete_stream(
    connection_id: str,
    stream_id: str,
):

    redis_available, connection = (
        await db.get_connection_lookup(connection_id)
    )

    if connection is None:
        return _connection_not_found_or_unavailable(
            redis_available
        )

    if stream_id not in connection.streams:
        return JSONResponse(
            status_code=404,
            content={
                "Error": "Stream id does not exist."
            },
        )

    if connection.default_stream_id == stream_id:
        return JSONResponse(
            status_code=400,
            content={
                "Error": (
                    f"Default stream for {connection_id} "
                    "cannot be deleted."
                )
            },
        )

    try:

        deleted = await db.delete_stream(
            connection_id,
            stream_id,
        )

    except utils.RedisPersistenceError as exc:

        logger.error(
            "Stream deletion persistence failed: %s",
            exc,
        )

        return JSONResponse(
            status_code=503,
            content={
                "Error": (
                    "Redis metadata persistence failed; "
                    "retry the request."
                )
            },
        )

    if not deleted:
        return JSONResponse(
            status_code=404,
            content={
                "Error": "Stream id does not exist."
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "Info": (
                f"{stream_id} stream has been deleted successfully."
            )
        },
    )

# ---------------------------------------------------------------------------
# WebSocket endpoint - continuous packet reception with diagnostics
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

    packet_count = 0
    total_packet_bytes = 0

    try:
    
        redis_available, connection = (
            await db.get_connection_lookup(connection_id)
        )

        if not redis_available:
            logger.warning(
                "[REDIS][UNAVAILABLE] ConnectionID=%s",
                connection_id,
            )
            return

        if connection is None:
            logger.warning(
                "[CLIENT][CONNECTION_NOT_FOUND] ConnectionID=%s",
                connection_id,
            )
            return

        logger.info(
            "[CLIENT][RECONNECT_SUCCESS] "
            "Existing ConnectionID=%s reused",
            connection_id,
        )

        while True:
            packet = await websocket.receive_bytes()

            packet_count += 1
            packet_size = len(packet)
            total_packet_bytes += packet_size

            await utils.write_packet(
                packet=packet,
                connection_id=connection_id,
            )

            logger.info(
                "[CLIENT][MESSAGE_PROCESSED] "
                "ConnectionID=%s | "
                "PacketNumber=%d | "
                "PacketSize=%d bytes",
                connection_id,
                packet_count,
                packet_size,
            )

    except WebSocketDisconnect as exc:
        logger.info(
        "[CLIENT][DISCONNECTED] ConnectionID=%s",
        connection_id,
    )
