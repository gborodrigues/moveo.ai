# moveo.ai – Real-Time Analytics Microservice

This repository contains a Kafka-backed real-time analytics system with two Go services:

- **Producer**: Exposes a REST endpoint that publishes user activity events to Kafka.
- **Consumer**: Consumes events, aggregates per-user counts, and stores results in PostgreSQL.

## Architecture

- Kafka topic: `incoming.user_activity`
- PostgreSQL table: `user_activity_aggregates` (per-user counts)

The aggregated results can be derived with SQL:

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

## Running locally with Docker

```bash
docker compose up --build
```

Send a sample event:

```bash
curl -X POST http://localhost:8080/activity \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "12345",
    "activity_type": "page_view",
    "timestamp": "2024-07-01T12:34:56Z",
    "metadata": {
      "page_url": "https://example.com/home",
      "referrer": "https://google.com"
    }
  }'
```

Query aggregates:

```bash
psql "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  -c "SELECT COUNT(*) AS users, COALESCE(SUM(page_view_count),0) AS page_views FROM user_activity_aggregates;"
```

## Configuration

Both services are configured by environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka brokers |
| `KAFKA_TOPIC` | `incoming.user_activity` | Kafka topic |
| `KAFKA_GROUP_ID` | `user-activity-consumer` | Consumer group id |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable` | Postgres connection string |
| `CONSUMER_WORKERS` | `3` | Number of consumer goroutines |
| `MAX_PROCESS_RETRY` | `5` | Max retries per message |
| `HTTP_ADDR` | `:8080` | Producer HTTP bind address |

## Building binaries

```bash
go build ./cmd/producer
go build ./cmd/consumer
```

## Notes

- Horizontal scaling is supported by running more consumer instances or increasing `CONSUMER_WORKERS`.
- Both services shutdown gracefully on SIGINT/SIGTERM.
