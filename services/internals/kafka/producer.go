package kafka

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
)

type TaskEventProducer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewTaskProducer(broker []string, topic string) (*TaskEventProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(broker, cfg)
	if err != nil {
		return nil, err

	}

	return &TaskEventProducer{
		producer: p,
		topic:    topic,
	}, nil
}

func (p *TaskEventProducer) PublishTaskReady(ctx context.Context, taskID string) error {
	payload := map[string]interface{}{
		"task_id": taskID,
	}
	data, _ := json.Marshal(payload)

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(taskID),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err := p.producer.SendMessage(msg)
	return err
}
