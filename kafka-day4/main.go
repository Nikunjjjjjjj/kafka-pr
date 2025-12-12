package main

import (
	"encoding/json"
	"kafka-day4/database"
	"kafka-day4/handlers"
	"kafka-day4/models"
	"kafka-day4/producer"
	"log"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

func main() {
	//for day6 connecting db for real day 4 go to day4 advance
	database.ConnectDB()
	// Init Kafka Producer
	producer.InitProducer([]string{"localhost:9092"})

	r := gin.Default()
	r.GET("/stats/product/:id", handlers.GetProductStats)

	r.POST("/track", func(c *gin.Context) {
		var event models.TrackEvent

		if err := c.BindJSON(&event); err != nil {
			c.JSON(400, gin.H{"error": "Invalid request"})
			return
		}

		// Convert event to JSON
		data, _ := json.Marshal(event)

		msg := &sarama.ProducerMessage{
			Topic: "events",
			Value: sarama.ByteEncoder(data),
			Key:   sarama.StringEncoder(event.Event), // Partitioning by event name
		}

		producer.Producer.Input() <- msg

		c.JSON(200, gin.H{
			"message": "Event received, publishing to Kafka",
		})
	})

	log.Println("Server started on :8080")
	r.Run(":8080")
}
