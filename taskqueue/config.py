import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

QUEUE_KEY = "taskqueue:main"
DELAYED_KEY = "taskqueue:delayed"              # sorted set: score = ready_at unix timestamp
DLQ_KEY = "taskqueue:dlq"
PROCESSED_KEY_PREFIX = "taskqueue:processed:"  # + job_id, with TTL -> idempotency

MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2                       # exponential: 2, 4, 8, 16, 32...
PROCESSED_TTL_SECONDS = 24 * 3600
