package api

import (
	"net/http"

	redisqueue "DistributedTaskScheduler/services/internals/redis"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type TaskRequest struct {
	Payload string `json:"payload" binding:"required"`
}

type TaskResponse struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

func CreateTask(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req TaskRequest
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request body",
			})
			return
		}
		taskID := uuid.New().String()

		err := redisqueue.PushTask(rdb, taskID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to queue task",
			})
			return
		}

		c.JSON(http.StatusOK, TaskResponse{
			TaskID: taskID,
			Status: "queued",
		})
	}
}
