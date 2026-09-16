import json
import time
import uuid
import redis

from . import config


class Broker:
    """Redis-backed broker implementing:
    - a main FIFO queue (list, BRPOP)
    - a delayed/retry queue (sorted set, score = ready_at)
    - a dead-letter queue for jobs that exceed MAX_RETRIES
    - idempotency tracking so at-least-once delivery doesn't double-process
    """

    def __init__(self, redis_url: str = config.REDIS_URL):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)

    # ---------- Producer side ----------
    def enqueue(self, task_name: str, payload: dict, idempotency_key: str | None = None) -> str:
        """Push a new job onto the main queue. Returns the job_id.

        If idempotency_key is given, re-enqueuing with the same key is safe:
        the worker will recognize the job_id and skip it if already processed.
        """
        job_id = idempotency_key or str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "task_name": task_name,
            "payload": payload,
            "attempts": 0,
            "enqueued_at": time.time(),
        }
        self.r.lpush(config.QUEUE_KEY, json.dumps(job))
        return job_id

    # ---------- Worker side ----------
    def dequeue(self, timeout: int = 5) -> dict | None:
        """Blocking pop from the main queue. Returns None on timeout (lets the
        worker loop check for shutdown signals periodically)."""
        result = self.r.brpop(config.QUEUE_KEY, timeout=timeout)
        if result is None:
            return None
        _, raw = result
        return json.loads(raw)

    def is_duplicate(self, job_id: str) -> bool:
        """Idempotency check: has this job_id already been successfully processed?"""
        return self.r.exists(f"{config.PROCESSED_KEY_PREFIX}{job_id}") == 1

    def mark_processed(self, job_id: str):
        self.r.set(f"{config.PROCESSED_KEY_PREFIX}{job_id}", "1", ex=config.PROCESSED_TTL_SECONDS)

    def schedule_retry(self, job: dict) -> str:
        """On failure: bump attempt count and either reschedule with exponential
        backoff or route to the dead-letter queue. Returns 'retry_scheduled' or 'dlq'."""
        job["attempts"] += 1
        if job["attempts"] > config.MAX_RETRIES:
            self.r.lpush(config.DLQ_KEY, json.dumps(job))
            return "dlq"

        delay = config.BASE_BACKOFF_SECONDS * (2 ** (job["attempts"] - 1))
        ready_at = time.time() + delay
        self.r.zadd(config.DELAYED_KEY, {json.dumps(job): ready_at})
        return "retry_scheduled"

    # ---------- Scheduler side (separate process) ----------
    def promote_ready_jobs(self) -> int:
        """Move jobs from the delayed sorted set back into the main queue once
        their backoff window has elapsed. Meant to be polled continuously."""
        now = time.time()
        ready = self.r.zrangebyscore(config.DELAYED_KEY, min=0, max=now)
        for raw in ready:
            pipe = self.r.pipeline()
            pipe.zrem(config.DELAYED_KEY, raw)
            pipe.lpush(config.QUEUE_KEY, raw)
            pipe.execute()
        return len(ready)

    def dlq_size(self) -> int:
        return self.r.llen(config.DLQ_KEY)

    def queue_size(self) -> int:
        return self.r.llen(config.QUEUE_KEY)
