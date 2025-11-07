# This is the broker
from flask import Flask, request, jsonify
import redis
from queue import Queue
import uuid
import json #using this because redis stores binary data

app = Flask(__name__)

# Connect to Redis
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

QUEUE_KEY = "task_queue"

@app.post("/submit")
def submit_task():
    data = request.json
    task = {
        "id": str(uuid.uuid4()),
        "type": data["type"],
        "payload": data.get("payload", {})
    }
    # Store task in Redis list (acts as queue)
    r.rpush(QUEUE_KEY, json.dumps(task))
    # Also store by ID for lookup
    r.hset("tasks", task["id"], json.dumps(task))
    return jsonify({"status": "queued", "task_id": task["id"]})

@app.get("/get")
def get_task():
    if task_queue.empty():
        return jsonify({"status": "empty"}), 204
    task = task_queue.get()
    return jsonify(task)
