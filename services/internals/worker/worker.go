package worker

import (
	"context"
	"encoding/json"
	"log"

	"DistributedTaskScheduler/services/internals/tasks"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	rdb *redis.Client
}

func NewWorker(rdb *redis.Client) *Worker {
	return &Worker{rdb: rdb}
}

func (w *Worker) HandleTaskReady(ctx context.Context, TaskID string) error {
	log.Println("Task received from Kafka:", TaskID)

	data, err := w.rdb.Get(ctx, getTaskKey(TaskID)).Result()
	if err != nil {
		log.Println(" failed to fetch task:", err)
		return err
	}

	var task tasks.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		log.Println(" failed to unmarshal task:", err)
		return err
	}

	// 2️⃣ Execute task
	if err := processTask(ctx, task); err != nil {
		log.Println("task execution failed:", err)
		return err
	}

	// 3️⃣ (optional) mark task as completed
	log.Println("✅ task completed:", task.ID)
	return nil
}

func getTaskKey(taskID string) string {
	return "task:" + taskID
}

func processTask(ctx context.Context, task tasks.Task) error {
	log.Printf("⚙️ Processing task [%s] type=%s", task.ID, task.Type)

	switch task.Type {

	case "sendEmail":
		payload, ok := task.Payload.(string)
		if !ok {
			log.Printf("unexpected payload type for sendEmail: %T", task.Payload)
			return nil
		}
		return tasks.SendEmail(ctx, payload)

	case "generateReport":
		payload, ok := task.Payload.(string)
		if !ok {
			log.Printf("unexpected payload type for generateReport: %T", task.Payload)
			return nil
		}
		return tasks.GenerateReport(ctx, payload)

	default:
		log.Println("⚠️ unknown task type:", task.Type)
		return nil
	}
}
