package scheduler

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Task struct {
	Id         int
	Type       func(context.Context)
	ExecuteAt  time.Time
	Status     string
	RetryCount int
}

func ScheduleTask(
	ctx context.Context,
	rdb *redis.Client,
	taskID int,
	executeAt time.Time,
) error {

	return rdb.ZAdd(ctx, "DelayedTasksKey", redis.Z{
		Score:  float64(executeAt.Unix()),
		Member: taskID,
	}).Err()
}
