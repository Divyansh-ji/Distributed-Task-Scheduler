package main

import (
	"context"
	"log"
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
	s := scheduler.NewScheduler(rdb, 500*time.Millisecond)
	go s.Start(ctx)

	// worker
	go worker.Worker(ctx, rdb)

	// api
	router := api.RegisterRoutes(rdb)

	log.Println("🚀 Gin API server running on :8080")
	router.Run(":8180")
}
