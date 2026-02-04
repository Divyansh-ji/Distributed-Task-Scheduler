package api

import (
	"database/sql"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func RegisterRoutes(rdb *redis.Client, db *sql.DB) *gin.Engine {
	router := gin.Default()

	router.POST("/tasks", CreateTask(rdb, db))
	router.GET("/tasks/:id", GetTaskByID(db))

	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	return router
}
