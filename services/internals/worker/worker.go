package worker

import (
	"DistributedTaskScheduler/services/internals/redis"
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
			processTask(taskID)
		}
	}
}
func processTask(taskID int) {

}
