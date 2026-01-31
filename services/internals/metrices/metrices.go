package metrices

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TasksCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_created_total",
		Help: "Total number of tasks created",
	})
	TasksCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_completed_total",
		Help: "Total number of tasks completed",
	})
	TasksFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_failed_total",
		Help: "Total number of tasks failed",
	})
	TasksRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_retried_total",
		Help: "Total number of tasks retried",
	})
	TasksDead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_dead_total",
		Help: "Total number of tasks dead",
	})
	TasksCancelled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tasks_cancelled_total",
		Help: "Total number of tasks cancelled",
	})
	TasksProcessingTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tasks_processing_time_seconds",
		Help:    "Time taken to process tasks",
		Buckets: []float64{1, 2, 5, 10, 20, 30, 40, 50, 60, 90, 120},
	})
)
