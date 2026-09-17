# Distributed Task Queue

A Redis-backed distributed task queue built from scratch in Python no Celery,
no RQ. Built to demonstrate the failure-handling that separates a toy job
runner from something you'd trust in production.

## What it handles

- **At-least-once delivery** via blocking pop (`BRPOP`) from a Redis list
- **Idempotency** — every job carries a `job_id` (or a caller-supplied
  `idempotency_key`); workers check a processed-set before running a handler,
  so re-delivery or duplicate enqueue never double-executes a job
- **Exponential backoff retries** — failed jobs move to a Redis sorted set
  keyed by `ready_at` timestamp, with delay doubling per attempt
  (2s, 4s, 8s, 16s, 32s)
- **Dead-letter queue** — jobs that exceed `MAX_RETRIES` land in a DLQ list
  for inspection instead of retrying forever
- **Decoupled scheduler** — a separate process polls the delayed set and
  promotes ready jobs back to the main queue, so workers stay simple
  consumers with no timer logic of their own
- **Horizontal scaling** — `BRPOP` is safe across multiple worker replicas;
  Redis guarantees each job is popped by exactly one worker

## Architecture

```
producer -> enqueue() -> [main queue] -> worker -> success -> mark processed
                                            |
                                          failure
                                            v
                                    [delayed queue] --(scheduler)--> back to main queue
                                            |
                                     (after MAX_RETRIES)
                                            v
                                          [DLQ]
```

## Run it

```bash
docker compose up --build
```

This starts Redis, 2 worker replicas, and the scheduler. In another terminal:

```bash
pip install -r requirements.txt
python examples/example_enqueue.py
```

Watch the worker logs — roughly 30% of `send_email` jobs fail on purpose
(see `taskqueue/example_tasks.py`) so you can see the retry/backoff path and,
eventually, the DLQ path in action.

## Run tests (no Redis required — uses fakeredis)

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Adding your own tasks

```python
from taskqueue.registry import task

@task("my_task_name")
def my_handler(payload: dict):
    ...
```

Import the module so the decorator runs, then `enqueue("my_task_name", {...})`
from anywhere.

## Design notes / what this demonstrates

- Handling **at-least-once semantics correctly** (idempotency, not just retries)
- **Exponential backoff** to avoid hammering a failing downstream dependency
- **Separation of concerns** between enqueue/dequeue, retry scheduling, and
  promotion — each is independently testable
- **Horizontal worker scaling** without coordination overhead
