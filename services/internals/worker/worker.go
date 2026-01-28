package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

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

func (w *Worker) HandleTaskReady(ctx context.Context, TaskID string) error {
	log.Println("Task received from Kafka:", TaskID)

	data, err := w.rdb.Get(ctx, tasks.TaskKey(TaskID)).Result()
	if err != nil {
		log.Println(" failed to fetch task:", err)
		return err
	}

	var task tasks.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		log.Println(" failed to unmarshal task:", err)
		return err
	}

	if err := processTask(ctx, task); err != nil {
		log.Println("task execution failed:", err)
		return err
	}

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
