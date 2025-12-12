package main

import "kafka-day5/worker"

func main() {
	worker.ConnectDB()
	worker.StartConsumer()
}
