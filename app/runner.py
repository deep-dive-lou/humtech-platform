"""
Always-on worker runner with two concurrent loops:
1) process_loop: claim/process jobs from bot.job_queue
2) send_loop: send pending outbound messages from bot.messages

Usage:
  python -m app.runner
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import sys
from datetime import datetime

from app.config import settings
from app.db import init_db_pool, close_db_pool, get_pool
from app.bot.jobs import claim_jobs, mark_done, mark_retry
from app.bot.processor import process_job
from app.bot.sender import send_pending_outbound

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Config from env (milliseconds)
WORKER_POLL_MIN_MS = int(os.getenv("WORKER_POLL_MIN_MS", "500"))
WORKER_POLL_MAX_MS = int(os.getenv("WORKER_POLL_MAX_MS", "2000"))
SEND_POLL_MIN_MS = int(os.getenv("SEND_POLL_MIN_MS", "300"))
SEND_POLL_MAX_MS = int(os.getenv("SEND_POLL_MAX_MS", "1500"))

# Batch sizes
PROCESS_BATCH_SIZE = int(os.getenv("PROCESS_BATCH_SIZE", "50"))
SEND_BATCH_SIZE = int(os.getenv("SEND_BATCH_SIZE", "50"))

# Shutdown flag
_shutdown_event: asyncio.Event | None = None


def _jitter_sleep_seconds(min_ms: int, max_ms: int) -> float:
    """Return a random sleep duration in seconds between min_ms and max_ms."""
    return random.randint(min_ms, max_ms) / 1000.0


async def process_loop() -> None:
    """Continuously claim and process jobs from bot.job_queue."""
    global _shutdown_event
    assert _shutdown_event is not None

    logger.info(
        "process_loop started (poll %d-%dms, batch %d)",
        WORKER_POLL_MIN_MS,
        WORKER_POLL_MAX_MS,
        PROCESS_BATCH_SIZE,
    )
    iteration = 0

    while not _shutdown_event.is_set():
        iteration += 1
        claimed_count = 0
        processed_count = 0
        failure_count = 0

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                # Claim jobs in a transaction
                async with conn.transaction():
                    jobs = await claim_jobs(conn, limit=PROCESS_BATCH_SIZE, locked_by=settings.worker_id)
                    claimed_count = len(jobs)

                # Process each job in its own transaction
                for job in jobs:
                    if _shutdown_event.is_set():
                        break
                    try:
                        async with conn.transaction():
                            await process_job(conn, job.job_id)
                            await mark_done(conn, job.job_id)
                        processed_count += 1
                    except Exception as e:
                        failure_count += 1
                        err = {"error": str(e), "job_id": job.job_id}
                        async with conn.transaction():
                            await mark_retry(conn, job.job_id, delay_seconds=30, error_obj=err)
                        logger.warning("process_loop job %s failed: %s", job.job_id, e)

        except Exception as e:
            logger.error("process_loop iteration %d error: %s", iteration, e)

        if claimed_count > 0:
            logger.info(
                "process_loop #%d: claimed=%d processed=%d failed=%d",
                iteration,
                claimed_count,
                processed_count,
                failure_count,
            )

        # Jittered sleep
        sleep_sec = _jitter_sleep_seconds(WORKER_POLL_MIN_MS, WORKER_POLL_MAX_MS)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=sleep_sec)
        except asyncio.TimeoutError:
            pass  # Normal timeout, continue loop

    logger.info("process_loop shutting down")


async def send_loop() -> None:
    """Continuously send pending outbound messages."""
    global _shutdown_event
    assert _shutdown_event is not None

    logger.info(
        "send_loop started (poll %d-%dms, batch %d)",
        SEND_POLL_MIN_MS,
        SEND_POLL_MAX_MS,
        SEND_BATCH_SIZE,
    )
    iteration = 0

    while not _shutdown_event.is_set():
        iteration += 1
        result = {"selected": 0, "sent": 0, "failed": 0}

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    result = await send_pending_outbound(conn, limit=SEND_BATCH_SIZE)

        except Exception as e:
            logger.error("send_loop iteration %d error: %s", iteration, e)

        if result.get("selected", 0) > 0:
            logger.info(
                "send_loop #%d: selected=%d sent=%d failed=%d",
                iteration,
                result.get("selected", 0),
                result.get("sent", 0),
                result.get("failed", 0),
            )

        # Jittered sleep
        sleep_sec = _jitter_sleep_seconds(SEND_POLL_MIN_MS, SEND_POLL_MAX_MS)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=sleep_sec)
        except asyncio.TimeoutError:
            pass  # Normal timeout, continue loop

    logger.info("send_loop shutting down")


def _handle_shutdown(signum, frame) -> None:
    """Signal handler for graceful shutdown."""
    global _shutdown_event
    sig_name = signal.Signals(signum).name
    logger.info("Received %s, initiating graceful shutdown...", sig_name)
    if _shutdown_event is not None:
        _shutdown_event.set()


async def main() -> None:
    """Main entry point: start both loops and handle shutdown."""
    global _shutdown_event
    _shutdown_event = asyncio.Event()

    # Register signal handlers
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("Starting HumTech worker runner (worker_id=%s)", settings.worker_id)

    # Initialize DB pool
    await init_db_pool()
    logger.info("Database pool initialized")

    try:
        # Run both loops concurrently
        await asyncio.gather(
            process_loop(),
            send_loop(),
        )
    finally:
        logger.info("Closing database pool...")
        await close_db_pool()
        logger.info("Worker runner stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # Already handled by signal handler