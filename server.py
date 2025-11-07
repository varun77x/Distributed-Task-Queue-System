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

@app.get("/status/<task_id>")
def get_status(task_id):
    data = r.hget("tasks", task_id)
    if not data:
        return jsonify({"error": "Task not found"}), 404
    task = json.loads(data)
    return jsonify({"id": task_id, "status": task["status"]})

@app.get("/get")
def get_task():
    task_data = r.lpop(QUEUE_KEY)  # Get next task
    if not task_data:
        return jsonify({"status": "empty"}), 204

    task = json.loads(task_data)
    task["status"] = "processing"
    r.hset("tasks", task["id"], json.dumps(task))  # Update status
    return jsonify(task)

@app.post("/ack")
def ack_task():
    """Worker acknowledgment endpoint"""
    data = request.json
    task_id = data.get("task_id")
    status = data.get("status", "done")

    if not task_id:
        return jsonify({"error": "Missing task_id"}), 400

    task_data = r.hget("tasks", task_id)
    if not task_data:
        return jsonify({"error": "Task not found"}), 404

    task = json.loads(task_data)
    task["status"] = status
    r.hset("tasks", task_id, json.dumps(task))

    return jsonify({"status": "acknowledged", "task_id": task_id, "new_status": status})