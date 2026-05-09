import json
import os
import time
import signal
import sys

from google.cloud import pubsub_v1, firestore

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
SUBSCRIPTION_ID = os.environ.get("PUBSUB_SUBSCRIPTION", "vote-sub")
COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "votes")

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
db = firestore.Client(project=PROJECT_ID)

processed = 0


def process_vote(message):
    """Decode the message, write to Firestore, ack on success."""
    global processed
    try:
        vote = json.loads(message.data.decode("utf-8"))
    except Exception as e:
        print(f"[worker] malformed message — dropping: {e}")
        message.ack()  # poison pill: do not retry forever
        return

    if not all(k in vote for k in ("user_id", "poll_id", "choice")):
        print(f"[worker] missing required fields — dropping")
        message.ack()
        return

    # Idempotent doc id: duplicate deliveries overwrite the same doc.
    doc_id = f"{vote['user_id']}_{vote['poll_id']}"
    vote["time_processed"] = time.time()

    try:
        db.collection(COLLECTION).document(doc_id).set(vote)
        message.ack()
        processed += 1
        latency = vote["time_processed"] - vote.get("time_created", vote["time_processed"])
        print(f"[worker] #{processed} stored {doc_id[:16]}... "
              f"choice={vote['choice']} e2e_latency={latency:.2f}s")
    except Exception as e:
        # Don't ack — Pub/Sub will redeliver after the ack deadline.
        print(f"[worker] firestore write failed, will retry: {e}")


def main():
    flow = pubsub_v1.types.FlowControl(max_messages=20)
    future = subscriber.subscribe(sub_path, callback=process_vote, flow_control=flow)
    print(f"[worker] listening on {sub_path}")

    def _shutdown(signum, _frame):
        print(f"[worker] signal {signum} — shutting down")
        future.cancel()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    with subscriber:
        try:
            future.result()
        except Exception as e:
            print(f"[worker] streaming pull error: {e}")
            future.cancel()
            raise


if __name__ == "__main__":
    main()
