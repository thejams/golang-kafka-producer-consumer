package kafka_handler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type kafkaGoConsumer struct {
	brokers  []string
	topic    string
	clientId string
}

//NewKafkaGoConsumerHandler initialice a new kafka handler
func NewKafkaGoConsumerHandler(brokers []string, topic string, clientId string) KafkaHandler {
	log.SetFormatter(&log.JSONFormatter{})
	return &kafkaGoConsumer{
		brokers:  brokers,
		topic:    topic,
		clientId: clientId,
	}
}

func (k kafkaGoConsumer) ConsumeMessage() (string, error) {
	// make a new reader that consumes from topic-A
	config := kafka.ReaderConfig{
		Brokers:         k.brokers,
		GroupID:         k.clientId,
		Topic:           k.topic,
		MinBytes:        10e3,            // 10KB
		MaxBytes:        10e6,            // 10MB
		MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: -1,
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-consumer", "method": "ConsumeMessage"}).Info("You pressed ctrl + C. User interrupted infinite loop.")
		os.Exit(0)
	}()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-consumer", "method": "ConsumeMessage"}).Error(err.Error())
			continue
		}

		value := m.Value
		logMessage := fmt.Sprintf("message at topic: %v | message: %v", m.Topic, string(value))
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-consumer", "method": "ConsumeMessage"}).Info(logMessage)
	}
}
