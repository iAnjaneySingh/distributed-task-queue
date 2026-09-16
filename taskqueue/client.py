"""Producer-facing API. Import and call `enqueue()` from anywhere in your app."""

from .broker import Broker

_broker = Broker()


def enqueue(task_name: str, payload: dict, idempotency_key: str | None = None) -> str:
    return _broker.enqueue(task_name, payload, idempotency_key=idempotency_key)
