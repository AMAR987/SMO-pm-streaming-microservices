import asyncio
import logging

from concurrent.futures import ProcessPoolExecutor

from util import (
    supervisor,
    stop_worker,
    PROCESS_POOL_WORKERS,
    BUFFER_MAX_SIZE,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("perf3gpp-main")


async def main():

    logger.info("PERF3GPP Decoder started")

    process_pool = ProcessPoolExecutor(
        max_workers=PROCESS_POOL_WORKERS
    )

    memory_buffer = asyncio.Queue(
        maxsize=BUFFER_MAX_SIZE
    )
    commit_queue = asyncio.Queue()

    retry_queue = asyncio.Queue(
        maxsize=BUFFER_MAX_SIZE
    )
    kafka_failure_event = asyncio.Event()

    try:

        await supervisor(
            process_pool,
            memory_buffer,
            retry_queue,
            kafka_failure_event,
            commit_queue,
        )
        logger.info(
            "PERF3GPP Decoder completed"
        )

    except asyncio.CancelledError:

        logger.info(
            "Application shutdown requested"
        )

        raise

    finally:

        logger.info("Application cleanup started")
        process_pool.shutdown(wait=True,cancel_futures=True,)

        logger.info("Application cleanup completed")
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("PERF3GPP Decoder stopped")