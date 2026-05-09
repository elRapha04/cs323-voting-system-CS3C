import os
import threading
from flask import Flask
import worker

app = Flask(__name__)


@app.route("/")
def health():
    return {"status": "ok", "processed": worker.processed}, 200


def _start_worker():
    worker.main()


threading.Thread(target=_start_worker, daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
