package worker

import (
	"DistributedTaskScheduler/services/internals/api"
	"DistributedTaskScheduler/services/internals/redis"
	"DistributedTaskScheduler/services/internals/tasks"
	"context"
	"log"
)

func StartWorker(n int) {
	for i := 0; i < n; i++ {
		go worker(ctx, rdb)

	}
}

func worker(ctx context.Context, rdb *redis.Client) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Worker stopping")
			return
		default:
			taskID, err := redis.PopTask(ctx, rdb)
			if err != nil {
				log.Println(err)
				continue
			}
			processTask(ctx, taskID)
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
