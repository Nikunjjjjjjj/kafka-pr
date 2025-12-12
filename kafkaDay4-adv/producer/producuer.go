package producer

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

// exported async producer
var Producer sarama.AsyncProducer

// message wrapper for retries
type retryableMessage struct {
	msg     *sarama.ProducerMessage
	attempt int
}

// config
const (
	MaxRetries       = 5
	RetryBackoffBase = 200 * time.Millisecond
)

// channels
var (
	retryCh chan retryableMessage
)

// Prometheus metrics (registered in InitProducer)
var (
	metricProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_produced_total",
		Help: "Total messages successfully produced to Kafka.",
	})
	metricFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_failed_total",
		Help: "Total messages failed after retries.",
	})
	metricRetried = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_messages_retried_total",
		Help: "Total message retry attempts.",
	})
	metricProduceLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_produce_latency_seconds",
		Help:    "Producer latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})
)

// InitProducer initializes async producer + retry worker + metrics
func InitProducer(brokers []string) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3 // sarama internal retry for transient errors
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Producer.Idempotent = false // true requires extra config and broker support; optional

	p, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}

	Producer = p
	retryCh = make(chan retryableMessage, 1000)

	// register metrics
	prometheus.MustRegister(metricProduced, metricFailed, metricRetried, metricProduceLatency)

	// success handler
	go func() {
		for s := range Producer.Successes() {
			// record produce latency if header included (producer can set ts in header if desired)
			metricProduced.Inc()
			log.Printf("[producer] delivered topic=%s partition=%d offset=%d key=%v",
				s.Topic, s.Partition, s.Offset, s.Key)
		}
	}()

	// error handler -> push into retryCh or mark failed
	go func() {
		for err := range Producer.Errors() {
			// try to find retry count header
			attempt := 0
			if v := getHeaderValue(err.Msg.Headers, "x-retry-attempt"); v != "" {
				// ignore parse error
				_ = attempt
			}
			metricRetried.Inc()
			retryCh <- retryableMessage{msg: err.Msg, attempt: attempt + 1}
			log.Printf("[producer] delivery error: %v, scheduling retry (attempt=%d)", err.Err, attempt+1)
		}
	}()

	// retry worker
	go func() {
		for r := range retryCh {
			if r.attempt > MaxRetries {
				metricFailed.Inc()
				log.Printf("[producer] giving up on message after %d attempts: topic=%s", r.attempt-1, r.msg.Topic)
				continue
			}

			// set/update header with attempt count
			setHeader(r.msg, "x-retry-attempt", []byte(stringInt(r.attempt)))

			// exponential backoff
			backoff := time.Duration(1<<uint(r.attempt-1)) * RetryBackoffBase
			time.Sleep(backoff)

			metricRetried.Inc()
			select {
			case Producer.Input() <- r.msg:
				// message enqueued for send, success/errors will be handled by routines above
			case <-time.After(5 * time.Second):
				// if queue is blocked, re-enqueue with incremented attempt (bounded)
				if r.attempt+1 > MaxRetries {
					metricFailed.Inc()
					log.Printf("[producer] enqueue timeout and max retries reached for topic=%s", r.msg.Topic)
				} else {
					r.attempt++
					retryCh <- r
				}
			}
		}
	}()

	// optionally context cancellation handling on shutdown not shown here
}

// helpers: getHeaderValue / setHeader / stringInt
func getHeaderValue(headers []sarama.RecordHeader, key string) string {
	for _, h := range headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func setHeader(msg *sarama.ProducerMessage, key string, value []byte) {
	// replace if exists
	for _, h := range msg.Headers {
		if string(h.Key) == key {
			h.Value = value
			return
		}
	}
	msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: value})
}

func stringInt(i int) string {
	return fmt.Sprintf("%d", i)
}
