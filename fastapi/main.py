import asyncio
import importlib.util
import logging
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket,  WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load the local fastapi/utils.py explicitly because the directory name
# "fastapi" can otherwise conflict with the installed FastAPI package.
utils_spec = importlib.util.spec_from_file_location(
    "hv_ves_fastapi_utils",
    PROJECT_ROOT / "fastapi" / "utils.py",
)
if utils_spec is None or utils_spec.loader is None:
    raise ImportError("Unable to load fastapi/utils.py")

utils = importlib.util.module_from_spec(utils_spec)
utils_spec.loader.exec_module(utils)



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hv-ves-fastapi")

app = FastAPI(title="HV-VES Ingestion", version="1.0.0")
db = utils.ClientInfo()
redis_monitor_task = None
kafka_monitor_task = None


class ConnectionRequest(BaseModel):
    stream_type: str
    serialization_format: str = "GPB"


class StreamRequest(BaseModel):
    stream_type: str
    serialization_format: str = "GPB"


@app.on_event("startup")
async def startup():
    global redis_monitor_task, kafka_monitor_task

    logger.info("Starting HV-VES ingestion service")

    # Redis and Kafka are optional during startup. If either service is down,
    # the FastAPI process remains available and background retry keeps trying.
    db.start_redis_sync()
    redis_monitor_task = asyncio.create_task(db.monitor_redis())
    kafka_monitor_task = asyncio.create_task(utils.connect_kafka_with_retry())

    logger.info("HV-VES ingestion service started; Redis/Kafka recovery runs in background")


@app.on_event("shutdown")
async def shutdown():
    global redis_monitor_task, kafka_monitor_task

    logger.info("Shutting down HV-VES ingestion service")

    for task in (redis_monitor_task, kafka_monitor_task):
        if task is not None:
            task.cancel()

    for task in (redis_monitor_task, kafka_monitor_task):
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    redis_monitor_task = None
    kafka_monitor_task = None

    await db.stop_redis_sync()
    await utils.stop_kafka()
    logger.info("HV-VES ingestion service stopped")


@app.post("/connections")
async def create_connection(request: ConnectionRequest):
    stream_type = utils.validate_stream_request(request)

    connection = await db.create_connection(
        stream_type=stream_type,
        serialization_format=request.serialization_format,
    )

    return JSONResponse(
        status_code=201,
        content=utils.connection_response(connection),
    )


@app.post("/connections/{connection_id}/streams")
async def create_stream(connection_id: str, request: StreamRequest):
    stream_type = utils.validate_stream_request(request)

    stream = await db.add_stream(
        connection_id=connection_id,
        stream_type=stream_type,
        serialization_format=request.serialization_format,
    )

    if stream is None:
        raise HTTPException(status_code=404, detail="Connection not found")

    return utils.stream_response(connection_id, stream)


@app.get("/connections/{connection_id}")
async def get_connection(connection_id: str):
    connection = await db.get_connection(connection_id)

    if connection is None:
        raise HTTPException(status_code=404, detail="Connection not found")

    return utils.connection_response(connection)


@app.delete("/connections/{connection_id}/streams/{stream_id}")
async def delete_stream(connection_id: str, stream_id: str):
    deleted = await db.delete_stream(connection_id, stream_id)

    if not deleted:
        raise HTTPException(status_code=404, detail="Stream not found")

    return {
        "status": "deleted",
        "connection_id": connection_id,
        "stream_id": stream_id,
    }


@app.websocket("/connections/{connection_id}")
async def websocket_endpoint(websocket: WebSocket, connection_id: str):
    # Metadata is read once when the WebSocket connection is established.
    # Redis is not accessed for every VES packet.
    connection = await db.get_connection(connection_id)

    if connection is None:
        await websocket.close(code=1000, reason="Connection not found")
        return

    if not connection.streams:
        await websocket.close(code=1000, reason="No stream configured")
        return

    stream = next(iter(connection.streams.values()))
    stream_type = stream.stream_type

    await websocket.accept()
    logger.info("WebSocket connected: %s", connection_id)

    try:
        while True:
            packet = await websocket.receive_bytes()

            if not packet:
                continue

            try:
                # Raw packet goes directly to the long-lived Kafka producer.
                await utils.write_packet(
                    packet=packet,
                    stream_type=stream_type,
                )
            except Exception:
                logger.exception(
                    "Kafka send failed for connection %s",
                    connection_id,
                )
                await websocket.close(
                    code=1000,
                    reason="Kafka unavailable",
                )
                return

    except WebSocketDisconnect as exc:
        logger.info(
            "WebSocket disconnected normally: %s code=%s",
            connection_id,
            getattr(exc, "code", 1000),
        )
    except Exception:
        logger.exception("WebSocket error: %s", connection_id)
        try:
            await websocket.close(code=1000, reason="Normal closure")
        except Exception:
            pass
    finally:
        logger.info("WebSocket disconnected: %s", connection_id)
