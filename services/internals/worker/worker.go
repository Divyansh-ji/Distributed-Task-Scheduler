package worker

import (
	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/tasks"
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

const ReadyQueueKey = "scheduler:ready_queue"

func worker(ctx context.Context, rdb *redis.Client) {
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
			go processTask(ctx, taskID)
		}
	}
}
func processTask(ctx context.Context, taskID string) {
	switch taskID {
	case "sendEmail":
		// Create a dummy api.TaskRequest or fetch the actual task details as needed
		var req api.TaskRequest
		go tasks.SendEmail(ctx, req)

	case "generateReport":
		var reqs api.TaskRequest

		go tasks.GenerateReport(ctx, reqs)

	}

}
