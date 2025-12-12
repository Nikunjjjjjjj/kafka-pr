package producer

import (
	"log"

	"github.com/IBM/sarama"
)

var Producer sarama.AsyncProducer

func InitProducer(brokers []string) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	p, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}

	Producer = p

	// Listen for success / errors
	go func() {
		for {
			select {
			case success := <-Producer.Successes():
				log.Printf("Message delivered → topic:%s partition:%d offset:%d",
					success.Topic, success.Partition, success.Offset)

			case err := <-Producer.Errors():
				log.Printf("Failed to deliver message: %v", err)
			}
		}
	}()
}
