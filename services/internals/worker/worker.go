package worker

import (
	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/tasks"
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

type Task struct {
	ID      string
	Type    string
	Payload api.CreateTaskRequest
}

const ReadyQueueKey = "scheduler:ready_queue"

func Worker(ctx context.Context, rdb *redis.Client) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Worker stopping")
			return

		default:
			result, err := rdb.BRPop(ctx, 0, ReadyQueueKey).Result()
			if err != nil {
				log.Println(err)
				continue
			}

			if len(result) < 2 {
				log.Println("Invalid BRPop result:", result)
				continue
			}

			taskID := result[1]

			var task Task
			raw, err := rdb.Get(ctx, "task:"+taskID).Bytes()
			if err != nil {
				if err == redis.Nil {
					log.Println("Task details missing for ID:", taskID)
				} else {
					log.Println("Failed to fetch task details:", err)
				}
				continue
			}

			if err := json.Unmarshal(raw, &task); err != nil {
				log.Println("Failed to unmarshal task:", err)
				continue
			}

			log.Printf("Processing task [%s] type=%s", task.ID, task.Type)

			// one task = one execution
			processTask(ctx, task)
		}
	}
}

func processTask(ctx context.Context, task Task) {
	switch task.Type {

	case "sendEmail":
		go tasks.SendEmail(ctx, task.Payload)

	case "generateReport":
		go tasks.GenerateReport(ctx, task.Payload)

	default:
		log.Println("Unknown task type:", task.Type)
	}
}
