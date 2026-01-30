package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"DistributedTaskScheduler/services/db/storage"
	"DistributedTaskScheduler/services/internals/kafka"
	"DistributedTaskScheduler/services/internals/scheduler"
	"DistributedTaskScheduler/services/internals/tasks"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	rdb      *redis.Client
	db       *sql.DB
	producer *kafka.TaskEventProducer
}

func NewWorker(rdb *redis.Client, db *sql.DB, producer *kafka.TaskEventProducer) *Worker {
	return &Worker{rdb: rdb, db: db, producer: producer}
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
			MaxRetries: taskRow.MaxRetries,
		}
		raw, _ := json.Marshal(task)
		_ = w.rdb.Set(ctx, tasks.TaskKey(taskID), raw, 1*time.Hour).Err()
	} else {
		log.Println("failed to fetch task from Redis:", err)
		return err
	}

	now := time.Now()
	_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "running", task.RetryCount+1,
		sql.NullTime{Time: now, Valid: true}, sql.NullTime{}, sql.NullString{}, task.MaxRetries)

	if err := processTask(ctx, task); err != nil {
		log.Println("task execution failed:", err)
		attempts := task.RetryCount + 1
		maxRetries := task.MaxRetries
		if maxRetries <= 0 {
			maxRetries = 3
		}

		if attempts <= maxRetries {
			// Retry: update once, re-enqueue, refresh cache
			_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "retry_scheduled", attempts,
				sql.NullTime{Time: now, Valid: true},
				sql.NullTime{},
				sql.NullString{String: err.Error(), Valid: true},
				task.MaxRetries)
			nextRetryAt := time.Now().Add(time.Duration(attempts) * time.Second)
			_ = scheduler.TaskAdd(ctx, w.rdb, taskID, nextRetryAt)
			task.RetryCount = attempts
			raw, _ := json.Marshal(task)
			_ = w.rdb.Set(ctx, tasks.TaskKey(taskID), raw, 1*time.Hour).Err()
			log.Println("task retried:", taskID)
			return nil
		}

		// Dead: update once, publish to DLQ
		_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "dead", attempts,
			sql.NullTime{Time: now, Valid: true},
			sql.NullTime{Time: time.Now(), Valid: true},
			sql.NullString{String: err.Error(), Valid: true},
			task.MaxRetries)
		if w.producer != nil {
			if dlqErr := w.producer.PublishTaskDLQ(ctx, taskID, err.Error(), attempts); dlqErr != nil {
				log.Println("failed to publish task DLQ:", dlqErr)
			}
		}
		log.Println("task dead:", taskID)
		return err
	}

	_ = storage.UpdateTaskStatus(ctx, w.db, taskID, "success", task.RetryCount+1,
		sql.NullTime{Time: now, Valid: true},
		sql.NullTime{Time: time.Now(), Valid: true},
		sql.NullString{},
		task.MaxRetries)
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
