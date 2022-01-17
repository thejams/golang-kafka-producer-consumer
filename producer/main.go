package main

import (
	"context"
	"fmt"
	"golang-kafka-producer-consumer/producer/controller"
	"log"
	"net/http"
	"os"
	"strings"

	httpServer "golang-kafka-producer-consumer/producer/http"
	kafka "golang-kafka-producer-consumer/producer/kafka"

	"github.com/gorilla/handlers"
)

func main() {
	port := os.Getenv("PORT")
	if len(strings.TrimSpace(port)) == 0 {
		port = ":5000"
	}

	brokers := os.Getenv("Kafka_Brokers")
	if len(strings.TrimSpace(brokers)) == 0 {
		brokers = "localhost:9092"
	}

	topic := os.Getenv("Kafka_Topic")
	if len(strings.TrimSpace(topic)) == 0 {
		topic = "myTopic"
	}

	clientId := os.Getenv("Client_ID")
	if len(strings.TrimSpace(clientId)) == 0 {
		clientId = "my-kafka-client"
	}

	ctx := context.Background()
	kafka_go_handler := kafka.NewKafkaGoProducerHandler(ctx, strings.Split(brokers, ","), topic, clientId)
	//sarama_kafka_handler := kafka.NewSaramaKafkaProducerHandler(strings.Split(brokers, ","), topic)

	// ctrl := controller.NewController(sarama_kafka_handler) // producer with github.com/Shopify/sarama
	ctrl := controller.NewController(kafka_go_handler) // producer with github.com/segmentio/kafka-go
	http_server := httpServer.NewHTTPServer(ctrl)

	fmt.Printf("server runing in port %v \n", port)
	log.Fatal(http.ListenAndServe(port, handlers.CORS(http_server.Credentials, http_server.Methods, http_server.Origins)(http_server.Router)))
}
