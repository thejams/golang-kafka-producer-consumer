package kafka_handler

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type saramaKafkaConsumer struct {
	topic    string
	consumer sarama.Consumer
}

//NewKafkaHandler initialice a new kafka handler
func NewSaramaKafkaConsumerHandler(brokers []string, topic string) (KafkaHandler, error) {
	log.SetFormatter(&log.JSONFormatter{})

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &saramaKafkaConsumer{
		consumer: conn,
		topic:    topic,
	}, nil
}

func (k saramaKafkaConsumer) ConsumeMessage() {
	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := k.consumer.ConsumePartition(k.topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-consumer", "method": "ConsumeMessage"}).Error(err.Error())
		panic(err)
	}
	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "ConsumeMessage"}).Info("Consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				quit <- true
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				logMsg := fmt.Sprintf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
				log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "ConsumeMessage"}).Info(logMsg)
			}
		}
	}()

	<-quit
	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "ConsumeMessage"}).Info(fmt.Sprintf("Processed: %d messages", msgCount))

	if err := k.consumer.Close(); err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-consumer", "method": "ConsumeMessage"}).Error(err.Error())
		panic(err)
	}
}

// CloseConsumer closes the consumer
func (k saramaKafkaConsumer) CloseConsumer() error {
	return nil
}
