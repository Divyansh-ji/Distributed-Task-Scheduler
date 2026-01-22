package api

import (
	"net/http"
	"time"

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

		// 🔹 Decide execution time
		executeAt := time.Now() // immediate execution
		// executeAt := time.Now().Add(10 * time.Second) // delayed example

		// 🔹 Add task to scheduler (ZSET)
		err := redisqueue.scheduler.TaskAdd(
			c.Request.Context(),
			rdb,
			taskID,
			executeAt,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to schedule task",
			})
			return
		}

		c.JSON(http.StatusOK, TaskResponse{
			TaskID: taskID,
			Status: "scheduled",
		})
	}
}
