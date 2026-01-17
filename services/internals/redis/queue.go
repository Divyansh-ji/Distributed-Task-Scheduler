package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const TaskQueue = "tasks:pending"

func PushTask(ctx context.Context, rdb *redis.Client, taskID string) error {
	return rdb.LPush(ctx, TaskQueue, taskID).Err()
}

func PopTask(ctx context.Context, rdb *redis.Client) (string, error) {
	result, err := rdb.BRPop(ctx, 0, TaskQueue).Result()
	if err != nil {
		return "", err
	}
	return result[1], nil
}
