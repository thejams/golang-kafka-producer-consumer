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
	"github.com/gorilla/mux"
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
	kafka_go_handler := kafka.NewKafkaGoHandler(ctx, strings.Split(brokers, ","), topic, clientId)
	//sarama_kafka_handler := kafka.NewSaramaKafkaHandler(strings.Split(brokers, ","), topic)

	// ctrl := controller.NewController(sarama_kafka_handler) // producer with github.com/Shopify/sarama
	ctrl := controller.NewController(kafka_go_handler) // producer with github.com/segmentio/kafka-go
	http_server := httpServer.NewHTTPServer(ctrl)

	router := mux.NewRouter().StrictSlash(true)
	router.Use(commonMiddleware)

	router.HandleFunc("/health", http_server.Health).Methods("GET")
	router.HandleFunc("/msg", http_server.CommitMessage).Methods("POST")

	credentials := handlers.AllowCredentials()
	methods := handlers.AllowedMethods([]string{"POST", "GET", "PUT", "DELETE"})
	origins := handlers.AllowedMethods([]string{"*"})

	fmt.Printf("server runing in port %v", port)
	fmt.Println()
	log.Fatal(http.ListenAndServe(port, handlers.CORS(credentials, methods, origins)(router)))

}

func commonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		next.ServeHTTP(w, r)
	})
}
