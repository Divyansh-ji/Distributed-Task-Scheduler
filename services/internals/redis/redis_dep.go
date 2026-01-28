package redis

import "fmt"

const taskKey = "task:%s"

func getTaskKey(taskID string) string {
	return fmt.Sprintf(taskKey, taskID)
}
