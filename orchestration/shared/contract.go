package shared

import "github.com/IBM/sarama"

type ConsumerContract interface {
	Consume(message *sarama.ConsumerMessage) error
}
