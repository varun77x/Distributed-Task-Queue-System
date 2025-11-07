from flask import Flask, request, jsonify
import redis
import uuid
import json
import traceback

app = Flask(__name__)

# Redis connection (with reconnect attempts)
try:
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    r.ping()
    print("✅ Connected to Redis")
except Exception as e:
    print("❌ Redis connection failed:", e)
    r = None

QUEUE_KEY = "task_queue"

def safe_json_load(data):
    try:
        return json.loads(data)
    except Exception:
        return None

@app.post("/submit")
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

        r.rpush(QUEUE_KEY, json.dumps(task))
        r.hset("tasks", task["id"], json.dumps(task))

        return jsonify({"status": "queued", "task_id": task["id"]})

    except redis.ConnectionError:
        return jsonify({"error": "Redis connection failed"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.get("/get")
def get_task():
    try:
        task_data = r.lpop(QUEUE_KEY)
        if not task_data:
            return jsonify({"status": "empty"}), 204

        task = safe_json_load(task_data)
        if not task:
            return jsonify({"error": "Corrupted task data"}), 500

        task["status"] = "processing"
        r.hset("tasks", task["id"], json.dumps(task))
        return jsonify(task)

    except redis.ConnectionError:
        return jsonify({"error": "Redis connection failed"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.post("/ack")
def ack_task():
    """Acknowledge task completion or retry"""
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

        # Handle different outcomes
        if status == "done":
            task["status"] = "done"
            r.hset("tasks", task_id, json.dumps(task))
            return jsonify({"status": "acknowledged", "task_id": task_id, "new_status": "done"})

        elif status == "failed":
            task["retries"] += 1
            if task["retries"] <= task["max_retries"]:
                task["status"] = "retrying"
                r.hset("tasks", task_id, json.dumps(task))
                r.rpush(QUEUE_KEY, json.dumps(task))  # Requeue
                return jsonify({
                    "status": "requeued",
                    "task_id": task_id,
                    "retries": task["retries"]
                })
            else:
                task["status"] = "failed_permanently"
                r.hset("tasks", task_id, json.dumps(task))
                return jsonify({
                    "status": "failed_permanently",
                    "task_id": task_id,
                    "retries": task["retries"]
                })

        else:
            return jsonify({"error": "Invalid status value"}), 400

    except redis.ConnectionError:
        return jsonify({"error": "Redis connection failed"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.get("/status/<task_id>")
def get_status(task_id):
    try:
        task_data = r.hget("tasks", task_id)
        if not task_data:
            return jsonify({"error": "Task not found"}), 404

        task = safe_json_load(task_data)
        if not task:
            return jsonify({"error": "Corrupted task data"}), 500

        return jsonify(task)

    except redis.ConnectionError:
        return jsonify({"error": "Redis connection failed"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
