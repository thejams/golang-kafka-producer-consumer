package kafka_handler

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	snappy "github.com/segmentio/kafka-go/snappy"
	log "github.com/sirupsen/logrus"
)

type kafkaGoProducer struct {
	topic    string
	ctx      context.Context
	producer *kafka.Writer
}

//NewKafkaGoProducerHandler initialice a new kafka handler
func NewKafkaGoProducerHandler(ctx context.Context, brokers []string, topic string, clientId string) KafkaHandler {
	log.SetFormatter(&log.JSONFormatter{})

	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: clientId,
	}

	config := kafka.WriterConfig{
		Brokers:          brokers,
		Topic:            topic,
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
	}

	return &kafkaGoProducer{
		topic:    topic,
		ctx:      ctx,
		producer: kafka.NewWriter(config),
	}
}

func (k kafkaGoProducer) PushMessage(value, key []byte) (string, error) {
	message := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	err := k.producer.WriteMessages(k.ctx, message)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-producer", "method": "PushMessage"}).Error(err.Error())
		return "", err
	}

	response := fmt.Sprintf("Message sent to topic topic(%s) | key(%s)", k.topic, key)
	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-producer", "method": "PushMessage"}).Info("ok")
	return response, nil
}

// CloseProducer close the producer
func (k kafkaGoProducer) CloseProducer() error {
	return k.producer.Close()
}
