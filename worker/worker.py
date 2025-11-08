import requests
import time
import random
import traceback
import os

# BROKER_URL = "http://localhost:5000"
BROKER_URL = os.getenv("BROKER_URL", "http://localhost:5000")
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN")
AUTH_HEADERS = {"X-API-Token": API_SECRET_TOKEN} if API_SECRET_TOKEN else {}


def process_task(task):
    """Simulate processing with random success/failure."""
    try:
        print(f"Processing {task['id']} (attempt {task['retries']+1}/{task['max_retries']})")
        time.sleep(random.uniform(1, 2))
        success = random.random() > 0.3  # 70% success rate
        if success:
            print(f"✅ Completed {task['id']}")
        else:
            print(f"❌ Failed {task['id']}")
        return success
    except Exception as e:
        print(f"⚠️ Error during task processing: {e}")
        traceback.print_exc()
        return False


def main_loop():
    while True:
        try:
            r = requests.get(f"{BROKER_URL}/get", timeout=10, headers=AUTH_HEADERS)
            if r.status_code == 204:
                time.sleep(1)
                continue
            elif not r.ok:
                print(f"⚠️ Broker returned error: {r.status_code}")
                time.sleep(2)
                continue

            task = r.json()
            success = process_task(task)
            ack_status = "done" if success else "failed"
            
            ack_attempts = 0
            acked = False
            while ack_attempts < 3 and not acked:
                try:
                    ack_resp = requests.post(
                        f"{BROKER_URL}/ack",
                        json={"id": task['id'], "status": ack_status},
                        headers=AUTH_HEADERS,
                        timeout=5
                    )
                    if ack_resp.ok:
                        print(f"✅ Successfully acknowledged task {task['id']} with status '{ack_status}'")
                        acked = True
                    else:
                        print(f"⚠️ Failed to ack task {task['id']}. Status: {ack_resp.status_code}. Retrying...")
                        ack_attempts += 1
                        time.sleep(2) # Wait before retrying
                except requests.exceptions.RequestException as e:
                    print(f"⚠️ Network error while acknowledging task {task['id']}: {e}. Retrying...")
                    ack_attempts += 1
                    time.sleep(2) # Wait before retrying

            if not acked:
                print(f"❌ CRITICAL: Could not acknowledge task {task['id']} after multiple retries. The task may be processed again.")
            
            try:
                ack_resp = requests.post(
                    f"{BROKER_URL}/ack",
                    json={"task_id": task["id"], "status": ack_status},
                    headers=AUTH_HEADERS
                )
                if ack_resp.ok:
                    print(f"Acknowledged {task['id']} as {ack_status}")
                else:
                    print(f"⚠️ ACK failed for {task['id']}: {ack_resp.status_code}")
            except requests.RequestException as e:
                print(f"⚠️ Error acknowledging task {task['id']}: {e}")
                traceback.print_exc()

        except requests.ConnectionError:
            print("❌ Broker unreachable. Retrying...")
            time.sleep(3)
        except requests.Timeout:
            print("⏱️ Timeout when fetching task. Retrying...")
            time.sleep(2)
        except KeyboardInterrupt:
            print("👋 Worker stopped by user.")
            break
        except Exception as e:
            print(f"💥 Unexpected error: {e}")
            traceback.print_exc()
            time.sleep(3)


if __name__ == "__main__":
    main_loop()
