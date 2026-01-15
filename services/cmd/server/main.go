package main

import (
	"log"

	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/redis"
)

func main() {
	rdb := redis.NewRedisClient()

	router := api.RegisterRoutes(rdb)

	log.Println("🚀 Gin API server running on :8080")
	router.Run(":8080")
}
