package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DelayedTaskKey = "scheduler:delayed_tasks"
	ReadyQueueKey  = "scheduler:ready_queue"
)

type Scheduler struct {
	rdb      *redis.Client
	interval time.Duration
}

func TaskAdd(ctx context.Context, rdb *redis.Client, taskID string, executeAt time.Time) error {
	return rdb.ZAdd(ctx, DelayedTaskKey, redis.Z{
		Score:  float64(executeAt.Unix()),
		Member: taskID,
	}).Err()
}

func NewScheduler(rdb *redis.Client, interval time.Duration) *Scheduler {
	return &Scheduler{
		rdb:      rdb,
		interval: interval,
	}

}
func (s *Scheduler) Start(ctx context.Context) {
	log.Println("scheduler started")

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("scheduler stopping")
			return

		case <-ticker.C:
			s.moveDueTasks(ctx)

		}
	}
}
func (s *Scheduler) moveDueTasks(ctx context.Context) {
	// immediate exit if shutting down
	if ctx.Err() != nil {
		return
	}

	redisCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	now := time.Now().Unix()

	taskIDs, err := s.rdb.ZRangeByScore(
		redisCtx,
		DelayedTaskKey,
		&redis.ZRangeBy{
			Min:   "-inf",
			Max:   fmt.Sprintf("%d", now),
			Count: 10,
		},
	).Result()

	if err != nil {
		if err == context.DeadlineExceeded {
			log.Println("redis timeout, skipping cycle")
			return
		}
		log.Println("error fetching due task:", err)
		return
	}

	for _, taskID := range taskIDs {
		if ctx.Err() != nil {
			return
		}

		if err := s.rdb.ZRem(redisCtx, DelayedTaskKey, taskID).Err(); err != nil {
			continue
		}
		if err := s.rdb.LPush(redisCtx, ReadyQueueKey, taskID).Err(); err != nil {
			continue
		}
		log.Println("task moved to ready queue:", taskID)
	}
}
