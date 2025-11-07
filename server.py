# This is the broker
from flask import Flask, request, jsonify
from queue import Queue
import uuid

app = Flask(__name__)
task_queue = Queue()

@app.post("/submit")
def submit_task():
    data = request.json
    task = {
        "id": str(uuid.uuid4()),
        "type": data["type"],
        "payload": data.get("payload", {})
    }
    task_queue.put(task)
    return jsonify({"status": "queued", "task_id": task["id"]})

@app.get("/get")
def get_task():
    if task_queue.empty():
        return jsonify({"status": "empty"}), 204
    task = task_queue.get()
    return jsonify(task)
