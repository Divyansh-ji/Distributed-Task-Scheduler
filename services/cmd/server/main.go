package main

import (
	"DistributedTaskScheduler/services/internals/api"
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
	// --------------------------------------------------
	// Root cancellable context
	// --------------------------------------------------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --------------------------------------------------
	// OS signal handling (Ctrl+C, Docker, K8s)
	// --------------------------------------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// --------------------------------------------------
	// Redis (long-lived dependency)
	// --------------------------------------------------
	rdb := redis.NewRedisClient()
	defer func() {
		log.Println("🔌 Closing Redis connection...")
		if err := rdb.Close(); err != nil {
			log.Printf("Redis close error: %v", err)
		}
	}()

	// --------------------------------------------------
	// Scheduler
	// --------------------------------------------------
	s := scheduler.NewScheduler(rdb, 500*time.Millisecond)
	go func() {
		log.Println("⏱ Scheduler started")
		s.Start(ctx)
	}()

	// --------------------------------------------------
	// Worker
	// --------------------------------------------------
	go func() {
		log.Println("👷 Worker started")
		worker.Worker(ctx, rdb)
	}()

	// --------------------------------------------------
	// HTTP Server (Gin)
	// --------------------------------------------------
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

	// --------------------------------------------------
	// Wait for shutdown signal
	// --------------------------------------------------
	<-sigCh
	log.Println("🛑 Shutdown signal received")

	// --------------------------------------------------
	// Graceful shutdown
	// --------------------------------------------------
	cancel() // stop scheduler & workers

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	log.Println("✅ Application shutdown completed")
}
