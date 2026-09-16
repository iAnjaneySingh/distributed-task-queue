"""Run this against a live Redis instance (docker compose up) to push sample jobs."""

from taskqueue.client import enqueue

if __name__ == "__main__":
    job_id = enqueue("send_email", {"to": "user@example.com", "subject": "Welcome!"})
    print(f"Enqueued job {job_id}")

    job_id2 = enqueue("resize_image", {"url": "https://example.com/pic.jpg", "size": "200x200"})
    print(f"Enqueued job {job_id2}")
