package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type UserActivity struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type Config struct {
	Brokers []string
	Topic   string
	Addr    string
}

func main() {
	cfg := Config{
		Brokers: splitEnv("KAFKA_BROKERS", "localhost:9092"),
		Topic:   getEnv("KAFKA_TOPIC", "incoming.user_activity"),
		Addr:    getEnv("HTTP_ADDR", ":8080"),
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/activity", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var activity UserActivity
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&activity); err != nil {
			respondError(w, http.StatusBadRequest, "invalid json payload")
			return
		}
		if err := validateActivity(&activity); err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		if activity.Timestamp.IsZero() {
			activity.Timestamp = time.Now().UTC()
		}
		payload, err := json.Marshal(activity)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to encode payload")
			return
		}
		msg := kafka.Message{
			Key:   []byte(activity.UserID),
			Value: payload,
			Time:  activity.Timestamp,
		}
		if err := writer.WriteMessages(r.Context(), msg); err != nil {
			log.Printf("failed to publish message: %v", err)
			respondError(w, http.StatusServiceUnavailable, "failed to publish event")
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("queued"))
	})

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("failed to shutdown http server: %v", err)
		}
		if err := writer.Close(); err != nil {
			log.Printf("failed to close kafka writer: %v", err)
		}
	}()

	log.Printf("producer listening on %s", cfg.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server error: %v", err)
	}
}

func validateActivity(activity *UserActivity) error {
	if strings.TrimSpace(activity.UserID) == "" {
		return errors.New("user_id is required")
	}
	if strings.TrimSpace(activity.ActivityType) == "" {
		return errors.New("activity_type is required")
	}
	return nil
}

func respondError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	_, _ = w.Write([]byte(message))
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
