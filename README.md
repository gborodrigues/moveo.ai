# moveo.ai – Real-Time Analytics Microservice

This repository contains a Kafka-backed real-time analytics system with two Go services:

- `Producer`: exposes a REST endpoint that publishes user activity events to Kafka.
- `Consumer`: consumes events, aggregates per-user counts, and stores results in PostgreSQL.

## Architecture

- Kafka topic: `incoming.user_activity`
- PostgreSQL table: `user_activity_aggregates` (per-user counts)

You can derive aggregated results with SQL, for example:

```sql
-- number of users
SELECT COUNT(*) FROM user_activity_aggregates;

-- number of page_view activities
SELECT COALESCE(SUM(page_view_count), 0) FROM user_activity_aggregates;
```

## Event schema

```json
{
  "user_id": "12345",
  "activity_type": "page_view",
  "timestamp": "2024-07-01T12:34:56Z",
  "metadata": {
    "page_url": "https://example.com/home",
    "referrer": "https://google.com"
  }
}
```

## Running locally (step-by-step)

Follow these steps to run the full stack (Kafka + Postgres + services) locally using Docker Compose.

1. Start Kafka, Zookeeper, Postgres, and both services

```bash
docker compose up --build -d
```

2. Wait a few seconds for containers to initialize. Check service logs if needed:

```bash
docker compose logs -f producer
docker compose logs -f consumer
```

3. Confirm the producer HTTP server is healthy

```bash
curl -sS http://localhost:8081/healthz
# should return: ok
```

4. Send a sample user activity event to the producer

```bash
curl -X POST http://localhost:8081/activity \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "12345",
    "activity_type": "page_view",
    "timestamp": "2024-07-01T12:34:56Z",
    "metadata": {"page_url": "https://example.com/home", "referrer": "https://google.com"}
  }'
```

5. Verify the consumer consumed and aggregated the event

Option A — connect from the host using `psql` (if installed):

```bash
psql "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" -c "SELECT * FROM user_activity_aggregates LIMIT 10;"
```

Option B — run `psql` inside the running Postgres container:

```bash
docker compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM user_activity_aggregates LIMIT 10;"
```

6. Stop the stack

```bash
docker compose down
```

## Building locally (without Docker)

If you prefer to build the Go binaries locally:

```bash
go build ./cmd/producer
go build ./cmd/consumer
```

Run the producer (example):

```bash
# set Kafka broker(s) and topic if needed
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=incoming.user_activity ./producer
```

Run the consumer (example):

```bash
DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" ./consumer
```

## Configuration (environment variables)

Both services read configuration from environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka brokers |
| `KAFKA_TOPIC` | `incoming.user_activity` | Kafka topic |
| `KAFKA_GROUP_ID` | `user-activity-consumer` | Consumer group id |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable` | Postgres connection string |
| `CONSUMER_WORKERS` | `3` | Number of consumer goroutines |
| `MAX_PROCESS_RETRY` | `5` | Max retries per message |
| `HTTP_ADDR` | `:8080` | Producer HTTP bind address |