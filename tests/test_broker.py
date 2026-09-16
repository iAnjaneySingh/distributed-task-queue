import json
import time

import fakeredis
import pytest

from taskqueue.broker import Broker
from taskqueue import config


@pytest.fixture
def broker():
    b = Broker.__new__(Broker)  # bypass __init__ so we can inject a fake redis client
    b.r = fakeredis.FakeRedis(decode_responses=True)
    return b


def test_enqueue_dequeue(broker):
    job_id = broker.enqueue("send_email", {"to": "a@b.com"})
    job = broker.dequeue(timeout=1)
    assert job["job_id"] == job_id
    assert job["task_name"] == "send_email"
    assert job["attempts"] == 0


def test_idempotency(broker):
    job_id = broker.enqueue("send_email", {"to": "a@b.com"})
    assert broker.is_duplicate(job_id) is False
    broker.mark_processed(job_id)
    assert broker.is_duplicate(job_id) is True


def test_retry_then_dlq(broker):
    job = {"job_id": "x1", "task_name": "send_email", "payload": {}, "attempts": 0}
    for _ in range(config.MAX_RETRIES):
        outcome = broker.schedule_retry(job)
        assert outcome == "retry_scheduled"
    outcome = broker.schedule_retry(job)
    assert outcome == "dlq"
    assert broker.dlq_size() == 1


def test_promote_ready_jobs(broker):
    job = {"job_id": "x2", "task_name": "send_email", "payload": {}, "attempts": 1}
    broker.r.zadd(config.DELAYED_KEY, {json.dumps(job): time.time() - 1})  # already "ready"
    promoted = broker.promote_ready_jobs()
    assert promoted == 1
    assert broker.queue_size() == 1


def test_exponential_backoff_increases_delay(broker):
    job = {"job_id": "x3", "task_name": "send_email", "payload": {}, "attempts": 0}
    broker.schedule_retry(job)  # attempts -> 1
    score_1 = broker.r.zscore(config.DELAYED_KEY, json.dumps(job))

    job2 = {"job_id": "x4", "task_name": "send_email", "payload": {}, "attempts": 2}
    broker.schedule_retry(job2)  # attempts -> 3, bigger delay
    score_2 = broker.r.zscore(config.DELAYED_KEY, json.dumps(job2))

    assert (score_2 - time.time()) > (score_1 - time.time())
