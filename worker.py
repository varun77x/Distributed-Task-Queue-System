import requests
import time
import random

BROKER_URL = "http://localhost:5000/get"

def process_task(task):
    print(f"Processing {task['id']} ({task['type']})")
    time.sleep(random.uniform(1, 3))  # Simulating work
    print(f"Completed {task['id']}")

while True:
    r = requests.get(BROKER_URL)
    if r.status_code == 204:
        time.sleep(1)
        continue
    task = r.json()
    process_task(task)
