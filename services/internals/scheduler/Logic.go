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

		case <-time.After(s.interval):
			s.moveDueTasks(ctx)

		}
	}
}
func (s *Scheduler) moveDueTasks(ctx context.Context) {
	log.Println("entered moveDueTasks")

	now := time.Now().Unix()

	taskID, err := s.rdb.ZRangeByScore(
		ctx,
		DelayedTaskKey,
		&redis.ZRangeBy{
			Min:   "-inf",
			Max:   fmt.Sprintf("%d", now),
			Count: 10,
		},
	).Result()
	log.Println("redis call returned")
	if err != nil {
		log.Println("error fetching due task:", err)
		return
	}
	for _, taskId := range taskID {

		if err := s.rdb.ZRem(ctx, DelayedTaskKey, taskId).Err(); err != nil {
			log.Println("failed to remove task :", taskId)
			continue
		}
		//push to ready Queue
		if err := s.rdb.LPush(ctx, ReadyQueueKey, taskId).Err(); err != nil {
			log.Println("failed to ready queue:", taskId)
			continue
		}
		log.Println("task moved to ready queue :", taskId)
	}
}
