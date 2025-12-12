package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		fmt.Printf("ConsumerGroup: message=%s, partition=%d, offset=%d\n",
			string(msg.Value), msg.Partition, msg.Offset)

		session.MarkMessage(msg, "")
	}

	return nil
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := []string{"localhost:9092"}
	groupID := "day3-group"
	// topics := []string{"test-topic"}
	topics := []string{"test-topic-3"}

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	handler := ConsumerGroupHandler{}
	fmt.Println("Consumer Group started...")

	for {
		err := consumerGroup.Consume(context.Background(), topics, handler)
		if err != nil {
			log.Printf("Error during consumption: %v", err)
		}
	}
}
