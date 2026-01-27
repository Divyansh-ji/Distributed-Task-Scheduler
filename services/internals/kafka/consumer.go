package kafka

import (
	"context"

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
