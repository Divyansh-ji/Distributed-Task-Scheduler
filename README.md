# Distributed Task Scheduler

A distributed task scheduler built in **Go** with Redis-backed scheduling, Kafka for task delivery, and PostgreSQL for persistence. Supports delayed execution, retries, fault tolerance, and a dead-letter queue (DLQ) for failed tasks.

## Features

- **Time-scheduled execution** — Tasks are scheduled via Redis sorted sets and dispatched when due.
- **Retries & fault tolerance** — Configurable `max_retries`; failed tasks can be retried or sent to a DLQ.
- **Observability** — Prometheus metrics (`/metrics`) for task lifecycle and processing latency.
- **Dashboard** — Web UI to create tasks and look up status by ID.

## Architecture

```
┌─────────────┐     POST /tasks      ┌─────────────┐     ZADD (executeAt)     ┌─────────────┐
│   Client    │ ──────────────────►  │  API (Gin)  │ ────────────────────────► │   Redis     │
│  / Dashboard│                      │             │                           │  (delayed   │
└─────────────┘                      │  PostgreSQL │◄──────────────────────────│   tasks)    │
                                     │  (task row)│     INSERT task            └──────┬──────┘
                                     └─────────────┘                                    │
                                                                                        │ scheduler
                                                                                        │ (poll, ZRANGE)
                                                                                        ▼
┌─────────────┐     consume           ┌─────────────┐     Publish              ┌─────────────┐
│  PostgreSQL │ ◄──────────────────  │   Worker    │ ◄───────────────────────│   Kafka     │
│  (status)   │     UpdateTaskStatus  │  (consumer) │     task.ready            │ task.ready  │
└─────────────┘                      └──────┬──────┘                           └─────────────┘
                                             │
                                             │ on max retries exceeded
                                             ▼
                                      ┌─────────────┐
                                      │  Kafka DLQ  │  task.dead
                                      └─────────────┘
```

1. **API** — Accepts `POST /tasks`, persists the task in PostgreSQL and Redis (ZSET by `executeAt`).
2. **Scheduler** — Periodically reads ready task IDs from Redis and publishes them to Kafka `task.ready`.
3. **Worker** — Consumes from `task.ready`, loads task from DB/Redis, executes, updates status; on final failure publishes to `task.dead`.

## Tech Stack

| Component   | Technology        |
|------------|-------------------|
| Language   | Go (Golang)       |
| API        | Gin               |
| Queue / Schedule | Redis (ZSET) |
| Messaging  | Kafka             |
| Database   | PostgreSQL        |
| Metrics    | Prometheus        |
| Frontend   | Embedded HTML/CSS/JS |

## Prerequisites

- **Go** 1.21+
- **PostgreSQL**
- **Redis**
- **Kafka** (broker for `task.ready` and `task.dead` topics)

## Environment Variables

Create a `.env` in the project root (or under `services/`). The app loads both.

| Variable        | Description                    | Example                    |
|----------------|--------------------------------|----------------------------|
| `DB_DSN` or `POSTGRES_DSN` | PostgreSQL connection string | `postgres://user:pass@localhost:5432/dbname?sslmode=disable` |
| `REDIS_ADDR`   | Redis address (optional)       | `localhost:6379`           |

Kafka brokers are currently hardcoded in `main.go` (`localhost:9092`); override in code or via env if needed.

## Database Setup

Run the migrations in order:

```bash
psql $DB_DSN -f services/db/migration/0001_create_task.sql
psql $DB_DSN -f services/db/migration/0002_max_retries.sql
```

## Running Locally

From the repository root:

```bash
go run ./services/cmd/server
```

- **API & dashboard:** http://localhost:8180  
- **Metrics:** http://localhost:8180/metrics  

Ensure PostgreSQL, Redis, and Kafka are running and that `task.ready` (and optionally `task.dead`) topics exist.

## API Endpoints

| Method | Path         | Description                    |
|--------|--------------|--------------------------------|
| POST   | `/tasks`     | Create a task (JSON: `type`, `payload`, optional `retryCount`, `nextRetryAt`). |
| GET    | `/tasks/:id` | Get task details and status.  |
| GET    | `/metrics`   | Prometheus metrics.           |
| GET    | `/`          | Dashboard (create task, look up by ID). |

### Create task example

```bash
curl -X POST http://localhost:8180/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":"{\"to\":\"user@example.com\"}","retryCount":2}'
```

## Metrics

Prometheus metrics are exposed at `GET /metrics`. Examples:

- `tasks_created_total` — Counter of tasks created via API.
- `tasks_completed_total` — Counter of tasks completed successfully.
- `tasks_retried_total` — Counter of tasks retried.
- `tasks_dead_total` — Counter of tasks sent to DLQ.
- `tasks_processing_time_seconds` — Histogram of task processing duration.

Use `prometheus.yml` in the repo to scrape the app (e.g. `targets: ["localhost:8180"]` for local Prometheus).

## Project Structure

```
DistributedTaskScheduler/
├── README.md
├── go.mod
├── prometheus.yml
├── .env
└── services/
    ├── cmd/server/
    │   └── main.go          # Entrypoint, wires API, scheduler, worker, Kafka, Redis, DB
    ├── db/
    │   ├── db.go            # PostgreSQL connection
    │   ├── migration/      # SQL migrations
    │   └── storage/
    │       └── task.go     # Task CRUD (CreateTask, GetTask, UpdateTaskStatus)
    ├── internals/
    │   ├── api/            # Gin routes, handlers (CreateTask, GetTaskByID)
    │   ├── kafka/          # Producer (task.ready, DLQ), consumer
    │   ├── metrices/       # Prometheus counters & histogram
    │   ├── redis/          # Redis client
    │   ├── scheduler/      # Redis ZSET scheduler, publishes to Kafka
    │   ├── tasks/          # Task type, retry helpers
    │   └── worker/         # Kafka consumer, executes tasks, updates DB, DLQ
    └── web/                # Embedded dashboard (HTML, CSS, JS)
```

## License

MIT (or as specified in the repository.)
