"""Example task handlers. Import this module to register them.
30% of send_email calls fail on purpose, to exercise the retry/backoff/DLQ path."""

import random
from .registry import task


@task("send_email")
def send_email(payload: dict):
    if random.random() < 0.3:
        raise RuntimeError("Simulated transient SMTP failure")
    print(f"Sending email to {payload.get('to')}: {payload.get('subject')}")


@task("resize_image")
def resize_image(payload: dict):
    print(f"Resizing image {payload.get('url')} to {payload.get('size')}")
