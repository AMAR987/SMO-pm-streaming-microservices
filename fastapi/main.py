import asyncio
import logging
import sys
import time
import os
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
kafka_queue_task: asyncio.Task | None = None


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
    global redis_monitor_task, kafka_monitor_task, kafka_queue_task
    logger.info("[STARTUP] HV-VES ingestion service starting | PID=%s", __import__("os").getpid())
    # Try Redis immediately so the service logs its initial state before accepting traffic.
    # If Redis is temporarily unavailable, background monitoring keeps retrying.
    redis_ready = await db.init_redis()
    if redis_ready:
        logger.info("[STARTUP][REDIS] Redis ready; loading existing connection inventory")
        await db.log_existing_connections()
    else:
        logger.warning("Redis is unavailable during startup; automatic reconnect is active")
    await db.recover_kafka_queue()
    redis_monitor_task = asyncio.create_task(db.monitor_redis(), name="redis-monitor")
    kafka_monitor_task = asyncio.create_task(utils.connect_kafka_with_retry(), name="kafka-monitor")
    kafka_queue_task = asyncio.create_task(utils.process_redis_kafka_queue(db), name="kafka-queue-worker")
    try:
        yield
    finally:
        logger.info("Shutting down HV-VES ingestion service")
        tasks = [t for t in (redis_monitor_task, kafka_monitor_task, kafka_queue_task) if t is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        redis_monitor_task = None
        kafka_monitor_task = None
        kafka_queue_task = None
        await db.stop_redis_sync()
        await utils.stop_kafka()
        logger.info("[SHUTDOWN] HV-VES ingestion service stopped")

app = FastAPI(title="HV-VES Ingestion", version="1.0.0", lifespan=lifespan)

@app.get("/health")
async def health():
    redis_state = await db.redis_status()
    redis_ok = redis_state["connected"]
    return JSONResponse(
        status_code=200 if redis_ok else 503,
        content={
            "status": "ok" if redis_ok else "degraded",
            "redis": "connected" if redis_ok else "unavailable",
            "redis_db": redis_state["db"],
            "connection_ttl_seconds": utils.CONNECTION_TTL,
            "pending_redis_writes": len(db.pending_upserts),
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
    stream_ids = [item.stream_id for item in parsed.stream_info_list]
    if len(stream_ids) != len(set(stream_ids)):
        return JSONResponse(status_code=400, content={"errorReason": "Duplicate streamId in streamInfoList"})
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

    logger.info("Connection created | ConnectionID=%s DefaultStreamID=%s", connection.connection_id, first.stream_id)
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
    """
    Keep the WebSocket open during temporary Redis outages.

    - Wait for Redis recovery if metadata lookup is unavailable.
    - Close only when Redis is available and the connection ID genuinely
      does not exist.
    - Once metadata is loaded, receive packets without further Redis lookups.
    - Preserve connection metadata when the WebSocket disconnects.
    """
    await websocket.accept()

    logger.info(
        "WebSocket accepted | ConnectionID=%s",
        connection_id,
    )

    delay = 1.0
    packet_count = 0
    total_bytes = 0

    try:
        # ---------------------------------------------------------
        # 1. Wait until Redis is available and connection metadata
        #    can be retrieved.
        # ---------------------------------------------------------
        while True:
            try:
                redis_available, connection = (
                    await db.get_connection_lookup(connection_id)
                )

                if connection is not None:
                    break

                if redis_available:
                    logger.warning(
                        "WebSocket connection ID not found | ConnectionID=%s",
                        connection_id,
                    )
                    await websocket.close(
                        code=1008,
                        reason="Connection id does not exist.",
                    )
                    return

                logger.warning(
                    "Redis unavailable; keeping WebSocket open and retrying "
                    "| ConnectionID=%s | RetryIn=%.1fs",
                    connection_id,
                    delay,
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Redis lookup failed; keeping WebSocket open "
                    "| ConnectionID=%s",
                    connection_id,
                )

            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)

        # ---------------------------------------------------------
        # 2. Connection metadata is available.
        #    No further Redis lookup is required in this loop.
        # ---------------------------------------------------------
        logger.info(
            "WebSocket ready | ConnectionID=%s | Streams=%s",
            connection_id,
            list(connection.streams),
        )

        while True:
            packet = await websocket.receive_bytes()

            if not packet:
                continue

            packet_count += 1
            total_bytes += len(packet)

            logger.info(
                "Packet received | ConnectionID=%s | Packet=%d | Bytes=%d",
                connection_id,
                packet_count,
                len(packet),
            )

            # -----------------------------------------------------
            # 3. Process packet.
            #    A Redis-related failure must not close the socket.
            # -----------------------------------------------------
            try:
                await utils.write_packet(
                    packet=packet,
                    connection_id=connection_id,
                )

                logger.info(
                    "Packet processed | ConnectionID=%s | Packet=%d",
                    connection_id,
                    packet_count,
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Packet processing failed; WebSocket remains open "
                    "| ConnectionID=%s | Packet=%d",
                    connection_id,
                    packet_count,
                )

                # Do not close the WebSocket because of a temporary
                # Redis/Kafka processing failure. Continue receiving
                # subsequent packets.

    except WebSocketDisconnect as exc:
        logger.info(
            "WebSocket disconnected | ConnectionID=%s | Code=%s "
            "| Packets=%d | TotalBytes=%d",
            connection_id,
            getattr(exc, "code", 1000),
            packet_count,
            total_bytes,
        )

    except asyncio.CancelledError:
        logger.info(
            "WebSocket task cancelled | ConnectionID=%s",
            connection_id,
        )
        raise

    except Exception:
        logger.exception(
            "WebSocket processing failed | ConnectionID=%s",
            connection_id,
        )

    finally:
        logger.info(
            "WebSocket handler finished | ConnectionID=%s "
            "| Packets=%d | TotalBytes=%d",
            connection_id,
            packet_count,
            total_bytes,
        )

