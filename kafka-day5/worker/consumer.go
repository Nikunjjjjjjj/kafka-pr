package worker

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

// this worker:

// Reads from all partitions,now when i hit day4 we can seeit here

// Unmarshals JSON

// Logs offset + partition

// Stores into DB
func StartConsumer() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_4_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest // or OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions("events")
	if err != nil {
		log.Fatal("Error fetching partitions:", err)
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition("events", partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				var e Event
				if err := json.Unmarshal(msg.Value, &e); err != nil {
					log.Println("JSON decode error:", err)
					continue
				}

				log.Printf("Received event: %+v | Offset: %d | Partition: %d\n",
					e, msg.Offset, msg.Partition)

				// _, dbErr := DB.Exec(
				// 	"INSERT INTO event_logs (user_id, event, product_id) VALUES ($1, $2, $3)",
				// 	e.UserID, e.Event, e.ProductID,
				// )
				_, dbErr := DB.Exec(`
					INSERT INTO product_stats (product_id, views)
					VALUES ($1, 1)
					ON CONFLICT (product_id)
					DO UPDATE SET views = product_stats.views + 1
				`, e.ProductID)

				if dbErr != nil {
					log.Println("DB insert error:", dbErr)
				}
			}
		}(pc)
	}

	select {} // keep worker running,
}
