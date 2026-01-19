package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type UserActivity struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type Config struct {
	Brokers         []string
	Topic           string
	GroupID         string
	DBConnString    string
	WorkerCount     int
	MaxProcessRetry int
}

func main() {
	cfg := Config{
		Brokers:         splitEnv("KAFKA_BROKERS", "localhost:9092"),
		Topic:           getEnv("KAFKA_TOPIC", "incoming.user_activity"),
		GroupID:         getEnv("KAFKA_GROUP_ID", "user-activity-consumer"),
		DBConnString:    getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		WorkerCount:     getEnvInt("CONSUMER_WORKERS", 3),
		MaxProcessRetry: getEnvInt("MAX_PROCESS_RETRY", 5),
	}

	db, err := sql.Open("postgres", cfg.DBConnString)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		log.Fatalf("failed to connect to database: %v", err)
	}
	cancel()

	if err := ensureSchema(db); err != nil {
		log.Fatalf("failed to ensure schema: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers: cfg.Brokers,
				Topic:   cfg.Topic,
				GroupID: cfg.GroupID,
			})
			defer func() {
				_ = reader.Close()
			}()
			consumeLoop(ctx, reader, db, cfg.MaxProcessRetry, workerID)
		}(i + 1)
	}

	<-ctx.Done()
	wg.Wait()
	log.Println("consumer shutdown complete")
}

func consumeLoop(ctx context.Context, reader *kafka.Reader, db *sql.DB, maxRetry int, workerID int) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("worker %d fetch error: %v", workerID, err)
			continue
		}
		if err := processWithRetry(ctx, db, msg, maxRetry, workerID); err != nil {
			log.Printf("worker %d failed to process message: %v", workerID, err)
			continue
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("worker %d commit error: %v", workerID, err)
		}
	}
}

func processWithRetry(ctx context.Context, db *sql.DB, msg kafka.Message, maxRetry int, workerID int) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetry; attempt++ {
		if err := processMessage(ctx, db, msg); err != nil {
			lastErr = err
			backoff := time.Duration(attempt*250) * time.Millisecond
			log.Printf("worker %d attempt %d failed: %v (retrying in %s)", workerID, attempt, err, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return nil
	}
	return lastErr
}

func processMessage(ctx context.Context, db *sql.DB, msg kafka.Message) error {
	var activity UserActivity
	if err := json.Unmarshal(msg.Value, &activity); err != nil {
		return err
	}
	if activity.UserID == "" || activity.ActivityType == "" {
		return errors.New("missing required fields")
	}
	if activity.Timestamp.IsZero() {
		activity.Timestamp = time.Now().UTC()
	}
	pageViewDelta := int64(0)
	if activity.ActivityType == "page_view" {
		pageViewDelta = 1
	}
	_, err := db.ExecContext(
		ctx,
		`INSERT INTO user_activity_aggregates (user_id, page_view_count, total_activity_count, last_activity)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (user_id) DO UPDATE
		 SET page_view_count = user_activity_aggregates.page_view_count + EXCLUDED.page_view_count,
		     total_activity_count = user_activity_aggregates.total_activity_count + EXCLUDED.total_activity_count,
		     last_activity = GREATEST(user_activity_aggregates.last_activity, EXCLUDED.last_activity)`,
		activity.UserID, pageViewDelta, int64(1), activity.Timestamp,
	)
	return err
}

func ensureSchema(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS user_activity_aggregates (
		user_id TEXT PRIMARY KEY,
		page_view_count BIGINT NOT NULL DEFAULT 0,
		total_activity_count BIGINT NOT NULL DEFAULT 0,
		last_activity TIMESTAMPTZ NOT NULL
	)`)
	return err
}

func splitEnv(key, fallback string) []string {
	value := getEnv(key, fallback)
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}