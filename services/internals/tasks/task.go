package tasks

type Task struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`

	RetryCount  int   `json:"retry_count"`
	NextRetryAt int64 `json:"next_retry_at"`
}

func TaskKey(taskID string) string {
	return "task:" + taskID
}
