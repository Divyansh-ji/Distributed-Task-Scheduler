package main

import (
	"context"
	"log"
	"time"

	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/redis"
	"DistributedTaskScheduler/services/internals/scheduler"
)

func main() {
	//we will create root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdb := redis.NewRedisClient()
	scheduler := scheduler.NewScheduler(rdb, 500*time.Millisecond)

	go scheduler.Start(ctx)

	router := api.RegisterRoutes(rdb)

	log.Println("🚀 Gin API server running on :8080")
	router.Run(":8080")
}
