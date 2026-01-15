package api

import (
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func RegisterRoutes(rdb *redis.Client) *gin.Engine {
	router := gin.Default()

	router.POST("/tasks", CreateTask(rdb))

	return router
}
