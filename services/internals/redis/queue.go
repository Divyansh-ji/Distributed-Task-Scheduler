package redis

import (
	"github.com/redis/go-redis/v9"
)

func PushTask(rdb *redis.Client, taskID string) error {
	return rdb.LPush(ctx, "tasks_queue", taskID).Err()
}

func PopTask(rdb *redis.Client) (string, error) {
	result, err := rdb.BRPop(ctx, 0, "tasks_queue").Result()
	if err != nil {
		return "", err
	}
	return result[1], nil
}
