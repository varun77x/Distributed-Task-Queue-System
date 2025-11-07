import requests
import time

BROKER_URL = "http://localhost:5000/get"

def process_task(task):
    print(f"Processing {task['id']} ({task['type']})")
    # TODO : Simulate work here
    time.sleep(2)
    print(f"Completed {task['id']}")

while True:
    r = requests.get(BROKER_URL)
    if r.status_code == 204:
        time.sleep(1)
        continue
    task = r.json()
    process_task(task)
