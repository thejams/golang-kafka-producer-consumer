package main

import (
	kafka "golang-kafka-producer-consumer/consumer/kafka"
	"os"
	"strings"
)

func main() {
	topic := os.Getenv("Kafka_Topic")
	if len(strings.TrimSpace(topic)) == 0 {
		topic = "myTopic"
	}

	brokers := os.Getenv("Kafka_Brokers")
	if len(strings.TrimSpace(brokers)) == 0 {
		brokers = "localhost:9092"
	}

	clientId := os.Getenv("Client_ID")
	if len(strings.TrimSpace(clientId)) == 0 {
		clientId = "my-kafka-client"
	}

	// sarama consumer
	// kafka_sarama_consumer_handler := kafka.NewSaramaKafkaConsumerHandler(strings.Split(brokers, ","), topic)
	// kafka_sarama_consumer_handler.ConsumeMessage()

	// kafka-go consumer
	kafka_go_consumer_handler := kafka.NewKafkaGoConsumerHandler(strings.Split(brokers, ","), topic, clientId)
	kafka_go_consumer_handler.ConsumeMessage()

}
