import os
import time
import uuid
import random
import socket
import requests

API_URL = os.environ.get("API_URL", "http://localhost:8080/vote")
EDGE_ID = os.environ.get("EDGE_ID", f"edge-{socket.gethostname()}")
POLL_ID = os.environ.get("POLL_ID", "poll_1")

# Toggle fault-injection scenarios via env vars for Part 5.
DUPLICATE_SENDS = int(os.environ.get("DUPLICATE_SENDS", "1"))   # >1 to duplicate
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))


def generate_vote():
    """Create a synthetic vote tagged with this edge node's identity."""
    return {
        "vote_id": str(uuid.uuid4()),         # unique per transmission
        "user_id": str(uuid.uuid4()),         # unique per simulated voter
        "poll_id": POLL_ID,
        "choice": random.choice(["A", "B", "C"]),
        "edge_id": EDGE_ID,
        "time_created": time.time(),
    }


def send_vote(vote):
    """POST vote to the Cloud Run API with exponential-backoff retries."""
    delay = 0.5
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(API_URL, json=vote, timeout=5)
            if r.status_code < 500:
                print(f"[{EDGE_ID}] sent {vote['user_id'][:8]} "
                      f"choice={vote['choice']} status={r.status_code}")
                return True
            raise RuntimeError(f"server {r.status_code}")
        except Exception as e:
            print(f"[{EDGE_ID}] attempt {attempt} failed: {e}")
            time.sleep(delay + random.random() * 0.25)
            delay = min(delay * 2, 8)
    print(f"[{EDGE_ID}] giving up on {vote['user_id']}")
    return False


def run_edge_node():
    print(f"[{EDGE_ID}] starting — target={API_URL} duplicates={DUPLICATE_SENDS}")
    while True:
        vote = generate_vote()
        # Fault injection: send the same vote multiple times to test idempotency.
        for _ in range(DUPLICATE_SENDS):
            send_vote(vote)
        time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    run_edge_node()
