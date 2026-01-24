package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/redis"
	"DistributedTaskScheduler/services/internals/scheduler"
	"DistributedTaskScheduler/services/internals/worker"
)

func main() {
	// root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// redis
	rdb := redis.NewRedisClient()

	// scheduler
	s := scheduler.NewScheduler(rdb, 10*time.Millisecond)
	go s.Start(ctx)

	// worker
	go worker.Worker(ctx, rdb)

	// api
	router := api.RegisterRoutes(rdb)

	// graceful shutdown (VERY IMPORTANT)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("🛑 Shutting down services...")
		cancel()
	}()

	log.Println("🚀 Gin API server running on :8080")
	router.Run(":8180")
}
