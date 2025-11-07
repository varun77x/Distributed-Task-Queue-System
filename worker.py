import requests
import time
import random

BROKER_URL = "http://localhost:5000/get"

def process_task(task):
    print(f"Processing {task['id']} ({task['type']})")
    time.sleep(random.uniform(1, 3))  # Simulating work
    # Simulate 30% failure rate
    success = random.random() > 0.3
    if success:
        print(f"✅ Completed {task['id']}")
    else:
        print(f"❌ Failed {task['id']}")
    return success

while True:
    r = requests.get(BROKER_URL)
    if r.status_code == 204:
        time.sleep(1)
        continue
    task = r.json()
    success = process_task(task)
    # Send acknowledgment
    ack_status = "done" if success else "failed"
    ack_resp = requests.post(
        f"{BROKER_URL}/ack",
        json={"task_id": task["id"], "status": ack_status}
    )

    if ack_resp.ok:
        print(f"Acknowledged task {task['id']} as {ack_status}")
    else:
        print(f"Failed to ack {task['id']}: {ack_resp.text}")
