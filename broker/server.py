from flask import Flask, request, jsonify
import redis
import uuid
import json
import traceback
import time
import threading
import os
from functools import wraps

app = Flask(__name__)

# --- CONFIG ---
MAIN_QUEUE = "task_queue"
DLQ = "dead_letter_queue"
PROCESSING_HASH = "processing_tasks"
VISIBILITY_TIMEOUT = 30  # seconds
redis_host = os.getenv("REDIS_HOST", "localhost")
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN")

# --- AUTHENTICATION DECORATOR ---
def require_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-API-Token")
        if not token or token != API_SECRET_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

# --- REDIS CONNECTION ---
try:
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    r.ping()
    print("✅ Connected to Redis")
except Exception as e:
    print("❌ Redis connection failed:", e)
    r = None


def safe_json_load(data):
    try:
        return json.loads(data)
    except Exception:
        return None


# --- API ENDPOINTS ---

@app.post("/submit")
@require_token
def submit_task():
    try:
        data = request.json
        if not data or "type" not in data:
            return jsonify({"error": "Missing required 'type' field"}), 400

        task = {
            "id": str(uuid.uuid4()),
            "type": data["type"],
            "payload": data.get("payload", {}),
            "status": "queued",
            "retries": 0,
            "max_retries": data.get("max_retries", 3)
        }

        r.rpush(MAIN_QUEUE, json.dumps(task))
        r.hset("tasks", task["id"], json.dumps(task))
        return jsonify({"status": "queued", "task_id": task["id"]})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.get("/get")
@require_token
def get_task():
    """Worker fetches the next available task."""
    try:
        task_data = r.lpop(MAIN_QUEUE)
        if not task_data:
            return jsonify({"status": "empty"}), 204

        task = safe_json_load(task_data)
        if not task:
            return jsonify({"error": "Corrupted task data"}), 500

        task["status"] = "processing"
        r.hset("tasks", task["id"], json.dumps(task))
        # Record processing timestamp
        r.hset(PROCESSING_HASH, task["id"], str(time.time()))

        return jsonify(task)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.post("/ack")
@require_token
def ack_task():
    """Worker acknowledgment or retry with DLQ fallback."""
    try:
        data = request.json
        if not data or "task_id" not in data:
            return jsonify({"error": "Missing task_id"}), 400

        task_id = data["task_id"]
        status = data.get("status", "done")

        task_data = r.hget("tasks", task_id)
        if not task_data:
            return jsonify({"error": "Task not found"}), 404

        task = safe_json_load(task_data)
        if not task:
            return jsonify({"error": "Corrupted task data"}), 500

        # Remove from processing list
        r.hdel(PROCESSING_HASH, task_id)

        if status == "done":
            task["status"] = "done"
            r.hset("tasks", task_id, json.dumps(task))
            return jsonify({"status": "acknowledged", "task_id": task_id})

        elif status == "failed":
            task["retries"] += 1
            if task["retries"] <= task["max_retries"]:
                task["status"] = "retrying"
                r.hset("tasks", task_id, json.dumps(task))
                r.rpush(MAIN_QUEUE, json.dumps(task))
                return jsonify({
                    "status": "requeued",
                    "task_id": task_id,
                    "retries": task["retries"]
                })
            else:
                task["status"] = "dead"
                r.hset("tasks", task_id, json.dumps(task))
                r.rpush(DLQ, json.dumps(task))
                return jsonify({
                    "status": "moved_to_dlq",
                    "task_id": task_id,
                    "retries": task["retries"]
                })
        else:
            return jsonify({"error": "Invalid status value"}), 400

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.get("/status/<task_id>")
@require_token
def get_status(task_id):
    try:
        data = r.hget("tasks", task_id)
        if not data:
            return jsonify({"error": "Task not found"}), 404
        task = safe_json_load(data)
        if not task:
            return jsonify({"error": "Corrupted task data"}), 500
        return jsonify(task)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.get("/dlq")
@require_token
def view_dlq():
    """Return current contents of DLQ."""
    try:
        tasks = r.lrange(DLQ, 0, -1)
        dlq_tasks = [safe_json_load(t) for t in tasks if safe_json_load(t)]
        return jsonify({"dlq_size": len(dlq_tasks), "tasks": dlq_tasks})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.post("/dlq/retry/<task_id>")
@require_token
def retry_from_dlq(task_id):
    """Move a task from DLQ back to main queue."""
    try:
        tasks = r.lrange(DLQ, 0, -1)
        found = None

        for t in tasks:
            task = safe_json_load(t)
            if task and task["id"] == task_id:
                found = task
                break

        if not found:
            return jsonify({"error": "Task not found in DLQ"}), 404

        r.lrem(DLQ, 0, json.dumps(found))
        found["retries"] = 0
        found["status"] = "requeued_from_dlq"
        r.hset("tasks", task_id, json.dumps(found))
        r.rpush(MAIN_QUEUE, json.dumps(found))
        return jsonify({"status": "requeued_from_dlq", "task_id": task_id})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# --- VISIBILITY TIMEOUT MONITOR ---
def visibility_watcher():
    """Background thread that requeues expired tasks."""
    print(f"🕒 Visibility monitor started (timeout = {VISIBILITY_TIMEOUT}s)")
    while True:
        try:
            all_processing = r.hgetall(PROCESSING_HASH)
            now = time.time()

            for task_id, ts in all_processing.items():
                age = now - float(ts)
                if age > VISIBILITY_TIMEOUT:
                    task_data = r.hget("tasks", task_id)
                    task = safe_json_load(task_data)
                    if not task:
                        continue

                    print(f"⚠️ Task {task_id} exceeded visibility timeout. Requeuing...")
                    task["status"] = "timeout_requeued"
                    r.hset("tasks", task_id, json.dumps(task))
                    r.rpush(MAIN_QUEUE, json.dumps(task))
                    r.hdel(PROCESSING_HASH, task_id)

            time.sleep(5)
        except Exception as e:
            print(f"⚠️ Visibility watcher error: {e}")
            traceback.print_exc()
            time.sleep(5)


# Start background watcher thread
threading.Thread(target=visibility_watcher, daemon=True).start()


if __name__ == "__main__":
    app.run(debug=True)
