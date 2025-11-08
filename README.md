# Distributed Task Queue System

A simple and scalable distributed task queue system built with Python, Flask, and Redis. 

It features a central broker API, multiple worker instances, a dead-letter queue for failed tasks, and a visibility timeout mechanism to handle worker failures gracefully.



The system consists of three main components orchestrated by Docker Compose: [file:1]

1.  **Broker (`server.py`)**: A Flask application that serves as the central API. It receives tasks from clients, adds them to a main queue, and provides endpoints for workers to fetch and acknowledge tasks. It also runs a background thread to monitor and handle task timeouts. [file:2]
2.  **Worker (`worker.py`)**: A Python script responsible for processing tasks. Multiple worker instances can be run concurrently. Each worker polls the broker for a new task, executes it, and sends back an acknowledgment (`done` or `failed`). [file:3]
3.  **Redis**: Acts as the in-memory data store for managing the task queues (main queue and DLQ) and storing task state. [file:1, file:2]

## Getting Started

### Prerequisites

-   Docker
-   Docker Compose

### Installation & Setup

1.  **Clone the repository:**
    ```
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```

2.  **Configure the API Token:**
    In the `docker-compose.yml` file, set a secure `API_SECRET_TOKEN` for both the `broker` and `worker` services. [file:1]

    ```
    environment:
      - API_SECRET_TOKEN=your-very-secret-token
    ```

3.  **Build and run the services:**
    This command will build the Docker images for the broker and worker and start all services in detached mode.

    ```
    docker-compose up --build -d
    ```

4.  **Scale the workers (Optional):**
    You can scale the number of worker instances up or down to match your workload.

    ```
    docker-compose up --scale worker=5 -d
    ```

## API Usage

All API endpoints require the `X-API-Token` header.

### Endpoints

-   `POST /submit`: Submit a new task.
-   `GET /get`: (For workers) Fetch the next available task from the queue.
-   `POST /ack`: (For workers) Acknowledge a completed or failed task.
-   `GET /status/<task_id>`: Check the status of a specific task.
-   `GET /dlq`: View all tasks in the Dead-Letter Queue.
-   `POST /dlq/retry/<task_id>`: Manually re-queue a task from the DLQ.



