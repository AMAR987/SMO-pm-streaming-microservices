import asyncio
from contextlib import asynccontextmanager
import importlib.util
import logging
import sys
from pathlib import Path
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from typing import Any
from fastapi.responses import JSONResponse
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

utils_spec = importlib.util.spec_from_file_location(
    "hv_ves_fastapi_utils",
    PROJECT_ROOT / "fastapi" / "utils.py",
)
if utils_spec is None or utils_spec.loader is None:
    raise ImportError("Unable to load fastapi/utils.py")
utils = importlib.util.module_from_spec(utils_spec)
sys.modules[utils_spec.name] = utils
utils_spec.loader.exec_module(utils)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hv-ves-fastapi")
db = utils.ClientInfo()
redis_monitor_task = None
kafka_monitor_task = None

@asynccontextmanager
async def lifespan(_app):
    await startup()
    try:
        yield
    finally:
        await shutdown()
app = FastAPI(title="HV-VES Ingestion", version="1.0.0", lifespan=lifespan)

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

async def startup():
    global redis_monitor_task, kafka_monitor_task
    logger.info("Starting HV-VES ingestion service")
    db.start_redis_sync()
    redis_monitor_task = asyncio.create_task(db.monitor_redis())
    kafka_monitor_task = asyncio.create_task(utils.connect_kafka_with_retry())
    logger.info("HV-VES ingestion service started; Redis/Kafka recovery runs in background")

async def shutdown():
    global redis_monitor_task, kafka_monitor_task
    logger.info("Shutting down HV-VES ingestion service")
    tasks = [task for task in (redis_monitor_task, kafka_monitor_task) if task is not None]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    redis_monitor_task = None
    kafka_monitor_task = None
    await db.stop_redis_sync()
    await utils.stop_kafka()
    logger.info("HV-VES ingestion service stopped")


@app.post("/connections")
async def create_connection(request: Request):
    """Create a connection and preserve the legacy v1.0 success response."""
    body = await _request_json(request)
    try:
        parsed = _validate(utils.ConnectionRequest, body)
    except (ValidationError, ValueError, TypeError) as exc:
        message = (
            "Either no data was provided or the data sent has error."
            if not body
            else "Data does not conform to schema"
        )
        logger.info("Connection request rejected: %s", exc)
        return JSONResponse(
            status_code=400,
            content={"errorReason": message},
        )
    stream_ids = [item.stream_id for item in parsed.stream_info_list]
    if len(stream_ids) != len(set(stream_ids)):
        return JSONResponse(status_code=400, content={"errorReason": "Data does not conform to schema"})
    first_stream = parsed.stream_info_list[0]
    connection = await db.create_connection(
        stream_type=utils.validate_stream_request(first_stream),
        serialization_format=first_stream.serialization_format,
        stream_id=first_stream.stream_id,
        producer_id=parsed.producer_id,
        meas_obj_dn=first_stream.meas_obj_dn,
        performance_metrics=first_stream.performance_metrics,
        vs_data_container=first_stream.vs_data_container,
    )
    for stream_info in parsed.stream_info_list[1:]:
        connection.streams[stream_info.stream_id] = utils.StreamInfo(
            stream_id=stream_info.stream_id,
            stream_type=utils.validate_stream_request(stream_info),
            serialization_format=stream_info.serialization_format,
            meas_obj_dn=stream_info.meas_obj_dn,
            performance_metrics=stream_info.performance_metrics,
            vs_data_container=stream_info.vs_data_container,
        )
    if len(parsed.stream_info_list) > 1:
        await db.save_connection(connection)
    logger.info(
        "Connection created | ConnectionID=%s StreamID=%s",
        connection.connection_id,
        first_stream.stream_id,
    )
    return JSONResponse(
        status_code=202,
        headers={"location": f"/connections/{connection.connection_id}"},
        content={"Info": "Connection to hv-ves has been accepted."},
    )


@app.post("/connections/{connection_id}/streams")
async def create_stream(connection_id: str, request: Request):
    body = await _request_json(request)
    try:
        parsed = _validate(utils.StreamRequest, body)
    except (ValidationError, ValueError, TypeError) as exc:
        logger.info("Stream request rejected: %s", exc)
        return JSONResponse(
            status_code=400,
            content={"Error": "Data does not conform to schema."},
        )
    redis_available, connection = await db.get_connection_lookup(connection_id)
    if connection is None:
        if not redis_available:
            logger.warning(
                "Connection metadata unavailable for %s because Redis is unavailable; retry later",
                connection_id,
            )
            return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
        return JSONResponse(status_code=400, content={"Error": "URI entered does not exist."})
    if parsed.stream_id in connection.streams:
        return JSONResponse(status_code=400, content={"Error": "New stream cannot be created."})
    stream = await db.add_stream(
        connection_id=connection_id,
        stream_type=utils.validate_stream_request(parsed),
        serialization_format=parsed.serialization_format,
        stream_id=parsed.stream_id,
        meas_obj_dn=parsed.meas_obj_dn,
        performance_metrics=parsed.performance_metrics,
        vs_data_container=parsed.vs_data_container,
    )
    if stream is None:
        logger.warning("Connection metadata became unavailable for %s while adding a stream", connection_id)
        return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
    logger.info("Stream added | ConnectionID=%s StreamID=%s", connection_id, stream.stream_id)
    return JSONResponse(status_code=200, content={"Info": "Stream successfully added."})


@app.get("/connections/{connection_id}")
async def get_connection(connection_id: str):
    redis_available, connection = await db.get_connection_lookup(connection_id)
    if connection is None:
        if not redis_available:
            logger.warning("GET connection %s deferred because Redis is unavailable", connection_id)
            return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
        return JSONResponse(status_code=400, content={"Error": "Connection id does not exist."})
    info = {
        stream_id: utils.stream_response_fields(stream)
        for stream_id, stream in connection.streams.items()
    }
    return JSONResponse(status_code=200, content={"info": info})


@app.delete("/connections/{connection_id}/streams/{stream_id}")
async def delete_stream(connection_id: str, stream_id: str):
    redis_available, connection = await db.get_connection_lookup(connection_id)
    if connection is None:
        if not redis_available:
            return JSONResponse(status_code=503, content={"Error": "Connection metadata temporarily unavailable."})
        return JSONResponse(status_code=400, content={"Error": "Connection id does not exist."})
    if stream_id not in connection.streams:
        return JSONResponse(status_code=400, content={"Error": "Stream id cannot be deleted as it does not exist."})
    if getattr(connection, "default_stream_id", None) == stream_id:
        return JSONResponse(
            status_code=400,
            content={"Error": f"Default stream for {connection_id} cannot be deleted."},
        )
    deleted = await db.delete_stream(connection_id, stream_id)
    if not deleted:
        return JSONResponse(status_code=400, content={"Error": "Stream id cannot be deleted as it does not exist."})
    return JSONResponse(status_code=200, content={"Info": f"{stream_id} stream has been deleted successfully."})


@app.websocket("/connections/{connection_id}")
async def websocket_endpoint(websocket: WebSocket, connection_id: str):
    await websocket.accept()
    logger.info("WebSocket accepted; resolving metadata for %s", connection_id)
    lookup_delay = 1.0
    lookup_cap = 10.0
    try:
        while True:
            redis_available, connection = await db.get_connection_lookup(connection_id)
            if connection is not None:
                break
            if redis_available:
                logger.warning("WebSocket connection ID not found: %s", connection_id)
                await websocket.close(code=1000, reason="Connection id does not exist.")
                return
            logger.warning(
                "Connection metadata unavailable for %s because Redis is down; keeping WebSocket open and retrying",
                connection_id,
            )
            await asyncio.sleep(lookup_delay)
            lookup_delay = min(lookup_delay * 2, lookup_cap)
        default_stream_id = getattr(connection, "default_stream_id", None)
        stream = connection.streams.get(default_stream_id) if default_stream_id else None
        if stream is None:
            stream = next(iter(connection.streams.values()), None)
        if stream is None:
            logger.error("No stream metadata available for connection %s", connection_id)
            await websocket.close(code=1000, reason="Stream metadata unavailable")
            return
        stream_type = stream.stream_type
        logger.info("WebSocket ready for raw VES packets: %s", connection_id)
        while True:
            packet = await websocket.receive_bytes()
            if not packet:
                continue
            await utils.write_packet(packet=packet, stream_type=stream_type)
    except WebSocketDisconnect as exc:
        logger.info(
            "WebSocket disconnected: %s code=%s",
            connection_id,
            getattr(exc, "code", 1000),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("WebSocket processing failed for %s", connection_id)
        try:
            await websocket.close(code=1011, reason="Internal server error")
        except Exception:
            logger.debug("WebSocket was already closed")
    finally:
        logger.info("WebSocket handler finished: %s", connection_id)
