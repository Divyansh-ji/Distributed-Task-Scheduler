package scheduler

import (
	"context"
	"time"
)

type Task struct {
	Id        int
	Type      func(context.Context)
	ExecuteAt time.Time
	Status    string
}
