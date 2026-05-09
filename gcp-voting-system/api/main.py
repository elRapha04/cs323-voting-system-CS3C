import json
import os
import time

from flask import Flask, request, jsonify
from google.cloud import pubsub_v1

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "vote-topic")

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

REQUIRED_FIELDS = ("user_id", "poll_id", "choice")
ALLOWED_CHOICES = {"A", "B", "C"}


@app.route("/", methods=["GET"])
def health():
    return {"status": "ok", "service": "vote-api"}, 200


@app.route("/vote", methods=["POST"])
def receive_vote():
    vote = request.get_json(silent=True)
    if not vote:
        return jsonify({"error": "Invalid payload"}), 400

    # Validate required fields.
    for field in REQUIRED_FIELDS:
        if field not in vote or not vote[field]:
            return jsonify({"error": f"Missing field: {field}"}), 400

    if vote["choice"] not in ALLOWED_CHOICES:
        return jsonify({"error": "choice must be A, B, or C"}), 400

    # Stamp ingestion time so the worker can compute end-to-end latency.
    vote["time_ingested"] = time.time()

    try:
        data = json.dumps(vote).encode("utf-8")
        future = publisher.publish(topic_path, data=data)
        msg_id = future.result(timeout=10)
        print(f"[api] published {msg_id} user={vote['user_id'][:8]} "
              f"choice={vote['choice']}")
        return jsonify({"status": "accepted", "message_id": msg_id}), 200
    except Exception as e:
        print(f"[api] publish failed: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
