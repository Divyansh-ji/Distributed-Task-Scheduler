package kafka

import (
	"log"

	"github.com/IBM/sarama"
)

func NewKafkaProducer(broker []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	Producer, err := sarama.NewSyncProducer(broker, config)
	if err != nil {
		log.Println("error in creating kafka producer:", err)
	}
	return Producer
}
