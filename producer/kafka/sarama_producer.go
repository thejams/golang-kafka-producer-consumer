package kafka_handler

import (
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type saramaKafkaProducer struct {
	topic    string
	producer sarama.SyncProducer
}

//NewSaramaKafkaProducerHandler initialice a new kafka handler
func NewSaramaKafkaProducerHandler(brokers []string, topic string) (KafkaHandler, error) {
	log.SetFormatter(&log.JSONFormatter{})

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "NewSaramaKafkaProducerHandler"}).Error(err.Error())
		return nil, err
	}
	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "NewSaramaKafkaProducerHandler"}).Info("ok")

	return &saramaKafkaProducer{
		topic:    topic,
		producer: conn,
	}, nil
}

func (k saramaKafkaProducer) PushMessage(message, key []byte) (string, error) {
	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.StringEncoder(message),
		Key:   sarama.StringEncoder(key),
	}

	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "PushMessage"}).Error(err.Error())
		return "", err
	}

	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "PushMessage"}).Info("ok")
	response := fmt.Sprintf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", k.topic, partition, offset)
	return response, nil
}

// CloseProducer close the producer
func (k saramaKafkaProducer) CloseProducer() error {
	return k.producer.Close()
}
