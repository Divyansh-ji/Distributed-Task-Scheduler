package main

import (
	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/kafka"
	"DistributedTaskScheduler/services/internals/redis"
	"DistributedTaskScheduler/services/internals/scheduler"
	"DistributedTaskScheduler/services/internals/worker"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	rdb := redis.NewRedisClient()
	defer func() {
		log.Println("🔌 Closing Redis connection...")
		if err := rdb.Close(); err != nil {
			log.Printf("Redis close error: %v", err)
		}
	}()
	producer := kafka.NewKafkaProducer([]string{"localhost:9092"})

	s := scheduler.NewScheduler(rdb, producer, 500*time.Millisecond)
	go func() {
		log.Println("⏱ Scheduler started")
		s.Start(ctx)
	}()

	go func() {
		log.Println("👷 Worker started")
		worker.Worker(ctx, rdb)
	}()

	router := api.RegisterRoutes(rdb)

	server := &http.Server{
		Addr:    ":8180",
		Handler: router,
	}

	go func() {
		log.Println("🚀 Gin API server running on :8180")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-sigCh
	log.Println("🛑 Shutdown signal received")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	log.Println("✅ Application shutdown completed")
}
