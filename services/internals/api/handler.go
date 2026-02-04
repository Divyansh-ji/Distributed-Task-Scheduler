package api

import (
	"DistributedTaskScheduler/services/db/storage"
	"DistributedTaskScheduler/services/internals/metrices"
	"DistributedTaskScheduler/services/internals/scheduler"
	"DistributedTaskScheduler/services/internals/tasks"
	"database/sql"
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

// TaskDetailsResponse for GET /tasks/:id (debugging & dashboards)
type TaskDetailsResponse struct {
	ID          string     `json:"id"`
	Type        string     `json:"type"`
	Payload     string     `json:"payload"`
	Status      string     `json:"status"`
	ScheduledAt time.Time  `json:"scheduledAt"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	FinishedAt  *time.Time `json:"finishedAt,omitempty"`
	Attempts    int        `json:"attempts"`
	LastError   *string    `json:"lastError,omitempty"`
	MaxRetries  int        `json:"maxRetries"`
}

const TaskKey = "task:%s"

func CreateTask(rdb *redis.Client, db *sql.DB) gin.HandlerFunc {
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

		// 🔹 Persist full task details by ID for worker consumption using shared tasks.Task
		taskEnvelope := tasks.Task{
			ID:          taskID,
			Type:        req.Type,
			Payload:     req.Payload,
			RetryCount:  req.RetryCount,
			NextRetryAt: req.NextRetryAt,
		}

		raw, err := json.Marshal(taskEnvelope)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to encode task",
			})
			return
		}
		err = storage.CreateTask(c.Request.Context(), db, storage.TaskRow{
			ID:          taskID,
			Type:        req.Type,
			Payload:     req.Payload,
			Status:      "queued",
			ScheduledAt: executeAt,
			Attempts:    0,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to create task in DB",
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
		metrices.TasksCreated.Inc()
	}
}

func GetTaskByID(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		taskID := c.Param("id")
		if taskID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "task id required"})
			return
		}
		taskRow, err := storage.GetTask(c.Request.Context(), db, taskID)
		if err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch task"})
			return
		}
		var startedAt, finishedAt *time.Time
		if taskRow.StartedAt.Valid {
			startedAt = &taskRow.StartedAt.Time
		}
		if taskRow.FinishedAt.Valid {
			finishedAt = &taskRow.FinishedAt.Time
		}
		var lastError *string
		if taskRow.LastError.Valid {
			lastError = &taskRow.LastError.String
		}
		c.JSON(http.StatusOK, TaskDetailsResponse{
			ID:          taskRow.ID,
			Type:        taskRow.Type,
			Payload:     taskRow.Payload,
			Status:      taskRow.Status,
			ScheduledAt: taskRow.ScheduledAt,
			StartedAt:   startedAt,
			FinishedAt:  finishedAt,
			Attempts:    taskRow.Attempts,
			LastError:   lastError,
			MaxRetries:  taskRow.MaxRetries,
		})
	}
}
