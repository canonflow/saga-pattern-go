package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

var (
	kafkaConsumers map[string]sarama.ConsumerGroup
	kafkaProducers map[string]sarama.SyncProducer
	consumerMu     sync.Mutex
	producerMu     sync.Mutex
)

func init() {
	kafkaConsumers = make(map[string]sarama.ConsumerGroup)
	kafkaProducers = make(map[string]sarama.SyncProducer)
}

func newKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	retryMax, err := strconv.Atoi(os.Getenv("KAFKA_RETRY"))
	if err != nil {
		retryMax = 3
	}
	config.Producer.Retry.Max = retryMax

	offsetReset := os.Getenv("KAFKA_OFFSET_RESET")
	if offsetReset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	return config
}

func getBrokers() []string {
	brokers := os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	return strings.Split(brokers, ",")
}

func GetKafkaConsumer(consumerGroup string) (sarama.ConsumerGroup, error) {
	consumerMu.Lock()
	defer consumerMu.Unlock()

	if consumer, ok := kafkaConsumers[consumerGroup]; ok {
		return consumer, nil
	}

	brokers := getBrokers()
	config := newKafkaConfig()

	consumer, err := sarama.NewConsumerGroup(brokers, consumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group %s: %w", consumerGroup, err)
	}

	kafkaConsumers[consumerGroup] = consumer
	return consumer, nil
}

func GetKafkaProducer(name string) (sarama.SyncProducer, error) {
	producerMu.Lock()
	defer producerMu.Unlock()

	if producer, ok := kafkaProducers[name]; ok {
		return producer, nil
	}

	brokers := getBrokers()
	config := newKafkaConfig()

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer %s: %w", name, err)
	}

	kafkaProducers[name] = producer
	return producer, nil
}

func CloseKafka() {
	for name, consumer := range kafkaConsumers {
		if err := consumer.Close(); err != nil {
			fmt.Printf("failed to close consumer %s: %v\n", name, err)
		}
	}
	for name, producer := range kafkaProducers {
		if err := producer.Close(); err != nil {
			fmt.Printf("failed to close producer %s: %v\n", name, err)
		}
	}
}
