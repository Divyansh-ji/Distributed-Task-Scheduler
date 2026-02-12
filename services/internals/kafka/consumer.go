package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type TaskEventHandler interface {
	HandleTaskReady(ctx context.Context, taskID string) error
}
type TaskEventConsumer struct {
	group   sarama.ConsumerGroup
	topics  []string
	ctx     context.Context
	handler TaskEventHandler
}

func NewTaskConsumer(
	ctx context.Context,
	brokers []string,
	groupID string,
	topics []string,
	handler TaskEventHandler,
) (*TaskEventConsumer, error) {

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}

	return &TaskEventConsumer{
		group:   group,
		topics:  topics,
		ctx:     ctx,
		handler: handler,
	}, nil
}


func (c *TaskEventConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Hook for future initialization or logging.
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *TaskEventConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	// Hook for future cleanup or logging.
	return nil
}

// HandleTaskReady starts the consumer loop and processes messages until the context is cancelled.
func (c *TaskEventConsumer) HandleTaskReady(ctx context.Context) {
	for {
		if err := c.group.Consume(ctx, c.topics, c); err != nil {
			log.Printf("Kafka consume error: %v", err)
		}


		if ctx.Err() != nil {
			return
		}
	}
}

func (c *TaskEventConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		var payload struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			session.MarkMessage(msg, "")
			continue
		}
		err := c.handler.HandleTaskReady(session.Context(), payload.TaskID)
		if err != nil {
			// TODO: implement retry or dead-letter queue handling.
			log.Printf("task handler error for task %s: %v", payload.TaskID, err)
		}
		session.MarkMessage(msg, "")

	}
	return nil
}
