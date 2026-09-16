import logging
import time

from .broker import Broker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scheduler")


def run(poll_interval: float = 1.0):
    """Continuously promotes delayed/retry jobs back to the main queue once
    their backoff window has elapsed. Runs as its own process/container so
    workers stay simple consumers."""
    broker = Broker()
    log.info("Scheduler started, polling delayed queue every %.1fs", poll_interval)
    while True:
        promoted = broker.promote_ready_jobs()
        if promoted:
            log.info("Promoted %d job(s) from delayed queue to main queue", promoted)
        time.sleep(poll_interval)


if __name__ == "__main__":
    run()
