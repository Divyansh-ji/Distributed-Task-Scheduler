package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"DistributedTaskScheduler/services/db/storage"
	"DistributedTaskScheduler/services/internals/tasks"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	rdb *redis.Client
	db  *sql.DB
}

func NewWorker(rdb *redis.Client, db *sql.DB) *Worker {
	return &Worker{rdb: rdb, db: db}
}

func (w *Worker) HandleTaskReady(ctx context.Context, taskID string) error {
	log.Println("Task received from Kafka:", taskID)

	var task tasks.Task

	data, err := w.rdb.Get(ctx, tasks.TaskKey(taskID)).Result()
	if err == nil {
		if err := json.Unmarshal([]byte(data), &task); err != nil {
			log.Println("failed to unmarshal task:", err)
			return err
		}
	} else if err == redis.Nil {
		taskRow, err := storage.GetTask(ctx, w.db, taskID)
		if err != nil {
			log.Println("failed to get task from DB:", err)
			return err
		}
		task = tasks.Task{
			ID:         taskRow.ID,
			Type:       taskRow.Type,
			Payload:    taskRow.Payload,
			RetryCount: taskRow.Attempts,
		}
		raw, _ := json.Marshal(task)
		_ = w.rdb.Set(ctx, tasks.TaskKey(taskID), raw, 1*time.Hour).Err()
	} else {
		log.Println("failed to fetch task from Redis:", err)
		return err
	}

	now := time.Now()
	_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "running", task.RetryCount+1,
		sql.NullTime{Time: now, Valid: true}, sql.NullTime{}, sql.NullString{})

	if err := processTask(ctx, task); err != nil {
		log.Println("task execution failed:", err)
		_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "failed", task.RetryCount+1,
			sql.NullTime{Time: now, Valid: true},
			sql.NullTime{Time: time.Now(), Valid: true},
			sql.NullString{String: err.Error(), Valid: true})
		return err
	}

	_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "success", task.RetryCount+1,
		sql.NullTime{Time: now, Valid: true},
		sql.NullTime{Time: time.Now(), Valid: true},
		sql.NullString{})
	log.Println("task completed:", task.ID)
	return nil
}

func processTask(ctx context.Context, task tasks.Task) error {
	log.Printf("⚙️ Processing task [%s] type=%s", task.ID, task.Type)

	switch task.Type {
	case "sendEmail":
		return tasks.SendEmail(ctx, task.Payload)

	case "generateReport":
		return tasks.GenerateReport(ctx, task.Payload)

	default:
		log.Println("⚠️ unknown task type:", task.Type)
		return nil
	}
}
