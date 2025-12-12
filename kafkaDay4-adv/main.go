package main

import (
	"encoding/json"
	"kafkaDay4-adv/model"
	"kafkaDay4-adv/producer"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// simple middleware to ensure and attach correlation id
func CorrelationIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		cid := c.GetHeader("X-Request-ID")
		if cid == "" {
			cid = uuid.NewString()
		}
		c.Set("correlation_id", cid)
		c.Writer.Header().Set("X-Request-ID", cid)
		c.Next()
	}
}

func main() {
	// init producer
	producer.InitProducer([]string{"localhost:9092"})

	r := gin.Default()
	r.Use(CorrelationIDMiddleware())

	// Prometheus metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.POST("/track", func(c *gin.Context) {
		var ev model.TrackEvent
		if err := c.BindJSON(&ev); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
			return
		}

		// marshal event
		data, _ := json.Marshal(ev)

		cid, _ := c.Get("correlation_id")

		// prepare sarama message
		msg := &sarama.ProducerMessage{
			Topic: "events",
			Key:   sarama.StringEncoder(ev.Event), // partitioning by event
			Value: sarama.ByteEncoder(data),
			Headers: []sarama.RecordHeader{
				{Key: []byte("X-Request-ID"), Value: []byte(cid.(string))},
				{Key: []byte("created_at"), Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			},
		}

		// send asynchronously
		//start := time.Now()
		producer.Producer.Input() <- msg

		// We don't wait for success — async. Metrics/Logging handled in producer package.
		c.JSON(http.StatusAccepted, gin.H{"status": "accepted", "request_id": cid})
	})

	log.Println("API listening :8080")
	r.Run(":8080")
}
