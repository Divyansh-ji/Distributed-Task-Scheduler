package redis

import (
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func PushTask(ctx gin.Context, rdb *redis.Client, taskID string) error {
	return rdb.LPush(ctx, "tasks_queue", taskID).Err()
}

func PopTask(ctx gin.Context, rdb *redis.Client) (string, error) {
	result, err := rdb.BRPop(ctx, 0, "tasks_queue").Result()
	if err != nil {
		return "", err
	}
	return result[1], nil
}
