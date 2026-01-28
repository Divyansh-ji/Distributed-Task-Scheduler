package api

import (
	"DistributedTaskScheduler/services/internals/scheduler"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type CreateTaskRequest struct {
	Type        string `json:"type" binding:"required"`
	Payload     string `json:"payload" binding:"required"`
	RetryCount  int    `json:"retryCount"`
	NextRetryAt int64  `json:"nextRetryAt"`
}

type TaskResponse struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

const TaskKey = "task:%s"

func CreateTask(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateTaskRequest
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request body",
			})
			return
		}
		taskID := uuid.New().String()

		executeAt := time.Now()

		// 🔹 Persist full task details by ID for worker consumption
		taskEnvelope := struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Payload string `json:"payload"`
		}{
			ID:      taskID,
			Type:    req.Type,
			Payload: req.Payload,
		}

		raw, err := json.Marshal(taskEnvelope)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to encode task",
			})
			return
		}

		// Store task details with a TTL to avoid indefinite buildup
		key := fmt.Sprintf(TaskKey, taskID)
		if err := rdb.Set(c.Request.Context(), key, raw, 1*time.Hour).Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to persist task",
			})
			return
		}

		// 🔹 Add task to scheduler (ZSET)
		err = scheduler.TaskAdd(
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
