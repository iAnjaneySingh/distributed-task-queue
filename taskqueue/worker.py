import logging
import signal

from .broker import Broker
from .registry import TASK_REGISTRY
from . import example_tasks  # noqa: F401  (import registers example tasks)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("worker")

_running = True


def _handle_shutdown(signum, frame):
    global _running
    log.info("Shutdown signal received, finishing current poll then exiting...")
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def run():
    broker = Broker()
    log.info("Worker started, waiting for jobs...")
    while _running:
        job = broker.dequeue(timeout=5)
        if job is None:
            continue

        job_id = job["job_id"]
        task_name = job["task_name"]

        if broker.is_duplicate(job_id):
            log.info("Skipping duplicate job %s (already processed — idempotency check)", job_id)
            continue

        handler = TASK_REGISTRY.get(task_name)
        if handler is None:
            log.error("No handler registered for task '%s' (job %s) — sending to DLQ", task_name, job_id)
            job["attempts"] = 999
            broker.schedule_retry(job)
            continue

        try:
            handler(job["payload"])
            broker.mark_processed(job_id)
            log.info("Job %s (%s) completed", job_id, task_name)
        except Exception as exc:
            log.warning("Job %s (%s) failed: %s", job_id, task_name, exc)
            outcome = broker.schedule_retry(job)
            log.info("Job %s -> %s (attempt %d)", job_id, outcome, job["attempts"])


if __name__ == "__main__":
    run()
