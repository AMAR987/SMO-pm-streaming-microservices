import asyncio
import logging
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import ValidationError
PROJECT_ROOT = Path(__file__).resolve().parent.parent
FASTAPI_DIR = Path(__file__).resolve().parent
for import_path in (str(FASTAPI_DIR), str(PROJECT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)
try:
    import util as utils
except ImportError as exc:
    raise ImportError("Unable to import util.py from the FastAPI project directory") from exc
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hv-ves-fastapi")
db = utils.ClientInfo()
redis_monitor_task: asyncio.Task | None = None
kafka_monitor_task: asyncio.Task | None = None

# Refresh Redis metadata every minute while a WebSocket is active. Each refresh
# resets the Redis key expiry to exactly 300 seconds.
METADATA_REFRESH_SECONDS = 60

async def _request_json(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}

def _validate(model, body):
    if hasattr(model, "model_validate"):
        return model.model_validate(body)
    return model.parse_obj(body)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global redis_monitor_task, kafka_monitor_task
    logger.info("Starting HV-VES ingestion service | PID=%s", __import__("os").getpid())
    # Start both Redis recovery loops before serving requests. The initial
    # connection attempt is non-fatal; monitor_redis retries indefinitely.
    redis_connected = await db.init_redis()
    if redis_connected:
        logger.info("Redis startup check succeeded")
    else:
        logger.warning("Redis is unavailable at startup; background reconnect is active")

    db.start_redis_sync()
    redis_monitor_task = asyncio.create_task(
        db.monitor_redis(), name="redis-monitor"
    )
    kafka_monitor_task = asyncio.create_task(
        utils.connect_kafka_with_retry(), name="kafka-monitor"
    )
    try:
        yield
    finally:
        logger.info("Shutting down HV-VES ingestion service")
        tasks = [t for t in (redis_monitor_task, kafka_monitor_task) if t is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        redis_monitor_task = None
        kafka_monitor_task = None
        await db.stop_redis_sync()
        await utils.stop_kafka()
        logger.info("HV-VES ingestion service stopped")

app = FastAPI(title="HV-VES Ingestion", version="1.0.0", lifespan=lifespan)

@app.get("/health")
async def health():
    redis_ok = await db.init_redis()
    return JSONResponse(
        status_code=200 if redis_ok else 503,
        content={
            "status": "ok" if redis_ok else "degraded",
            "redis": "connected" if redis_ok else "unavailable",
            "redis_pending_writes": len(db.pending_upserts),
            "redis_pending_deletes": len(db.pending_deletes),
            "connection_ttl_seconds": utils.CONNECTION_TTL,
        },
    )

@app.post("/connections")
async def create_connection(request: Request):
    body = await _request_json(request)
    try:
        parsed = _validate(utils.ConnectionRequest, body)
    except (ValidationError, ValueError, TypeError) as exc:
        logger.info("Connection request rejected: %s", exc)
        message = "Either no data was provided or the data sent has error." if not body else "Data does not conform to schema"
        return JSONResponse(status_code=400, content={"errorReason": message})

    first = parsed.stream_info_list[0]
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
        # Add any additional streams and persist the complete metadata again.
        for item in parsed.stream_info_list[1:]:
            if item.stream_id in connection.streams:
                raise ValueError(f"Duplicate streamId: {item.stream_id}")
            connection.streams[item.stream_id] = utils.StreamInfo(
                stream_id=item.stream_id,
                stream_type=utils.validate_stream_request(item),
                serialization_format=item.serialization_format,
                meas_obj_dn=item.meas_obj_dn,
                performance_metrics=item.performance_metrics,
                vs_data_container=item.vs_data_container,
            )
        if len(parsed.stream_info_list) > 1 and not await db.save_connection(connection):
            db.pending_upserts.pop(connection.connection_id, None)
            logger.error("Could not persist all streams for connection %s", connection.connection_id)
            return JSONResponse(status_code=503, content={"errorReason": "Redis metadata persistence failed; retry the request."})
    except utils.RedisPersistenceError as exc:
        logger.error("Connection creation failed: %s", exc)
        return JSONResponse(status_code=503, content={"errorReason": "Redis is unavailable; connection was not created. Retry the request."})
    except (ValidationError, ValueError, TypeError) as exc:
        logger.info("Connection request rejected: %s", exc)
        return JSONResponse(status_code=400, content={"errorReason": "Data does not conform to schema"})

    logger.info(
        "CONNECTION_CREATED_AND_PERSISTED | ConnectionID=%s DefaultStreamID=%s RedisKey=%s TTL=%s",
        connection.connection_id, first.stream_id,
        db._redis_key(connection.connection_id), utils.CONNECTION_TTL,
    )
    return JSONResponse(status_code=201, headers={"Location": f"/connections/{connection.connection_id}"}, content={"Info": "Connection to hv-ves has been accepted.", "connection_id": connection.connection_id})

@app.post("/connections/{connection_id}/streams")
async def create_stream(connection_id: str, request: Request):
    body = await _request_json(request)
    try:
        parsed = _validate(utils.StreamRequest, body)
    except (ValidationError, ValueError, TypeError) as exc:
        logger.info("Stream request rejected: %s", exc)
        return JSONResponse(status_code=400, content={"Error": "Data does not conform to schema."})
    redis_available, connection = await db.get_connection_lookup(connection_id)
    if connection is None:
        if not redis_available:
            return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
        return JSONResponse(status_code=404, content={"Error": "Connection id does not exist."})
    if parsed.stream_id in connection.streams:
        return JSONResponse(status_code=400, content={"Error": "New stream cannot be created; streamId already exists."})
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
        logger.error("Stream creation failed for %s: %s", connection_id, exc)
        return JSONResponse(status_code=503, content={"Error": "Redis metadata persistence failed; retry the request."})
    if stream is None:
        return JSONResponse(status_code=404, content={"Error": "Connection id does not exist."})
    logger.info("Stream added | ConnectionID=%s StreamID=%s", connection_id, stream.stream_id)
    return JSONResponse(status_code=200, content={"Info": "Stream successfully added."})

@app.get("/connections/{connection_id}")
async def get_connection(connection_id: str):
    redis_available, connection = await db.get_connection_lookup(connection_id)
    if connection is None:
        if not redis_available:
            return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
        return JSONResponse(status_code=404, content={"Error": "Connection id does not exist."})
    info = {sid: utils.stream_response_fields(stream) for sid, stream in connection.streams.items()}
    return JSONResponse(status_code=200, content={"info": info})

@app.delete("/connections/{connection_id}/streams/{stream_id}")
async def delete_stream(connection_id: str, stream_id: str):
    redis_available, connection = await db.get_connection_lookup(connection_id)
    if connection is None:
        if not redis_available:
            return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
        return JSONResponse(status_code=404, content={"Error": "Connection id does not exist."})
    if stream_id not in connection.streams:
        return JSONResponse(status_code=404, content={"Error": "Stream id does not exist."})
    if connection.default_stream_id == stream_id:
        return JSONResponse(status_code=400, content={"Error": f"Default stream for {connection_id} cannot be deleted."})
    try:
        deleted = await db.delete_stream(connection_id, stream_id)
    except utils.RedisPersistenceError as exc:
        logger.error("Stream deletion persistence failed: %s", exc)
        return JSONResponse(status_code=503, content={"Error": "Redis metadata persistence failed; retry the request."})
    if not deleted:
        return JSONResponse(status_code=404, content={"Error": "Stream id does not exist."})
    return JSONResponse(status_code=200, content={"Info": f"{stream_id} stream has been deleted successfully."})


@app.websocket("/connections/{connection_id}")
async def websocket_endpoint(websocket: WebSocket, connection_id: str):
    """Accept a new or reconnecting WebSocket using its connection ID."""
    await websocket.accept()
    logger.info(
        "WEBSOCKET_CONNECT_REQUEST | ConnectionID=%s",
        connection_id,
    )
    delay = 1.0
    try:
        while True:
            redis_available, connection = (
                await db.get_connection_lookup(connection_id)
            )
            if not redis_available:
                logger.warning(
                    "Redis temporarily unavailable; waiting for recovery | "
                    "ConnectionID=%s",
                    connection_id,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue
            if connection is None:
                logger.warning(
                    "Connection metadata not found | ConnectionID=%s",
                    connection_id,
                )
                await websocket.close(
                    code=1008,
                    reason="Connection metadata not found",
                )
                return
            break
        logger.info(
            "WEBSOCKET_READY | ConnectionID=%s | Streams=%s",
            connection_id,
            list(connection.streams),
        )
        delay = 1.0
        last_refresh = time.monotonic()
        while True:
            try:
                packet = await asyncio.wait_for(
                    websocket.receive_bytes(),
                    timeout=METADATA_REFRESH_SECONDS,
                )
            except asyncio.TimeoutError:
                # Refresh metadata even when the connection is idle.
                if not await db.update_activity(connection_id):
                    logger.warning(
                        "Redis metadata refresh failed | ConnectionID=%s",
                        connection_id,
                    )
                last_refresh = time.monotonic()
                continue
            if not packet:
                continue
            await utils.write_packet(
                packet=packet,
                connection_id=connection_id,
            )
            # Periodically refresh connection metadata.
            if (
                time.monotonic() - last_refresh
                >= METADATA_REFRESH_SECONDS
            ):
                if not await db.update_activity(connection_id):
                    logger.warning(
                        "Redis metadata refresh failed | ConnectionID=%s",
                        connection_id,
                    )
                last_refresh = time.monotonic()
    except WebSocketDisconnect as exc:
        logger.info(
            "WEBSOCKET_DISCONNECTED | ConnectionID=%s | Code=%s",
            connection_id,
            getattr(exc, "code", 1000),
        )
    except asyncio.CancelledError:
        logger.info(
            "WEBSOCKET_CANCELLED | ConnectionID=%s",
            connection_id,
        )
        raise
    except Exception:
        logger.exception(
            "WEBSOCKET_PROCESSING_ERROR | ConnectionID=%s",
            connection_id,
        )
        try:
            await websocket.close(
                code=1011,
                reason="Internal server error",
            )
        except Exception:
            logger.debug(
                "WebSocket already closed | ConnectionID=%s",
                connection_id,
            )
    finally:
        logger.info(
            "WEBSOCKET_HANDLER_FINISHED | ConnectionID=%s",
            connection_id,
        )

