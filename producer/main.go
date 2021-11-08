package main

import (
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

	kafka_handler := kafka.NewKafkaHandler(brokers, topic)

	/* var wg sync.WaitGroup
	wg.Add(1) */
	// kafka_handler.MakeConsumer()

	ctrl := controller.NewController(kafka_handler)
	http_server := httpServer.NewHTTPServer(ctrl)

	router := mux.NewRouter().StrictSlash(true)
	router.Use(commonMiddleware)

	router.HandleFunc("/health", http_server.Health)
	router.HandleFunc("/msg", http_server.CommitMessage)

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
