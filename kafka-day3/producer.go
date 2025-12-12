package main

import (
	"log"

	"github.com/IBM/sarama"
)

// func main() {
// 	brokers := []string{"localhost:9092"}
// 	topic := "day3-topic"

// 	config := sarama.NewConfig()
// 	config.Producer.Return.Successes = true

// 	producer, err := sarama.NewSyncProducer(brokers, config)
// 	if err != nil {
// 		log.Fatalf("Failed to create producer: %v", err)
// 	}
// 	defer producer.Close()

// 	for i := 1; i <= 10; i++ {
// 		msg := fmt.Sprintf("Message %d from Go", i)
// 		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
// 			Topic: topic,
// 			Value: sarama.StringEncoder(msg),
// 		})

// 		if err != nil {
// 			log.Printf("Failed to send message: %v", err)
// 		} else {
// 			fmt.Printf("Sent → %s | Partition=%d Offset=%d\n", msg, partition, offset)
// 		}
// 	}
// }

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// IMPORTANT — correct broker address
	brokers := []string{"localhost:9092"}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()
	//topics := []string{"test-topic-3"}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test-topic-3",
		Value: sarama.StringEncoder("Hello Kafka"),
	})
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Println("Message produced!")
}
