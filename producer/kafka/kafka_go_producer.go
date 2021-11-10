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
	brokers  []string
	topic    string
	clientId string
	ctx      context.Context
}

//NewKafkaGoProducerHandler initialice a new kafka handler
func NewKafkaGoProducerHandler(ctx context.Context, brokers []string, topic string, clientId string) KafkaHandler {
	log.SetFormatter(&log.JSONFormatter{})
	return &kafkaGoProducer{
		brokers:  brokers,
		topic:    topic,
		clientId: clientId,
		ctx:      ctx,
	}
}

func (k kafkaGoProducer) GetProducer() (*kafka.Writer, error) {
	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: k.clientId,
	}

	config := kafka.WriterConfig{
		Brokers:          k.brokers,
		Topic:            k.topic,
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
	}

	w := kafka.NewWriter(config)
	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-producer", "method": "GetProducer"}).Info("ok")
	return w, nil
}

func (k kafkaGoProducer) PushMessage(value []byte) (string, error) {
	producer, err := k.GetProducer()
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-producer", "method": "PushMessage"}).Error(err.Error())
		return "", err
	}
	defer producer.Close()

	message := kafka.Message{
		Key:   nil,
		Value: value,
		Time:  time.Now(),
	}

	err = producer.WriteMessages(k.ctx, message)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-producer", "method": "PushMessage"}).Error(err.Error())
		return "", err
	}

	response := fmt.Sprintf("Message sent to topic topic(%s)\n", k.topic)
	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "kafka-go-producer", "method": "PushMessage"}).Info("ok")
	return response, nil
}
