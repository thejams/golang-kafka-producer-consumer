package kafka_handler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type kafkaGoConsumer struct {
	brokers  []string
	topic    string
	clientId string
	consumer *kafka.Reader
}

//NewKafkaGoConsumerHandler initialice a new kafka handler
func NewKafkaGoConsumerHandler(brokers []string, topic string, clientId string) KafkaHandler {
	log.SetFormatter(&log.JSONFormatter{})

	config := kafka.ReaderConfig{
		Brokers:         brokers,
		GroupID:         clientId,
		Topic:           topic,
		MinBytes:        10e3,            // 10KB
		MaxBytes:        10e6,            // 10MB
		MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: -1,
	}

	return &kafkaGoConsumer{
		brokers:  brokers,
		topic:    topic,
		clientId: clientId,
		consumer: kafka.NewReader(config),
	}
}

func (k kafkaGoConsumer) ConsumeMessage() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-consumer", "method": "ConsumeMessage"}).Info("You pressed ctrl + C. User interrupted infinite loop.")
		os.Exit(0)
	}()

	for {
		m, err := k.consumer.ReadMessage(context.Background())
		if err != nil {
			log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-consumer", "method": "ConsumeMessage"}).Error(err.Error())
			continue
		}

		logMessage := fmt.Sprintf("message at topic: %s | key: %s | in partition: %s | message: %s", m.Topic, string(m.Key), strconv.Itoa(m.Partition), string(m.Value))
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-consumer", "method": "ConsumeMessage"}).Info(logMessage)
	}
}

// CloseConsumer closes the consumer
func (k kafkaGoConsumer) CloseConsumer() error {
	return k.consumer.Close()
}
