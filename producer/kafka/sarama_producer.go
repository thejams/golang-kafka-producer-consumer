package kafka_handler

import (
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type saramaKafkaProducer struct {
	brokers []string
	topic   string
}

//NewSaramaKafkaProducerHandler initialice a new kafka handler
func NewSaramaKafkaProducerHandler(brokers []string, topic string) KafkaHandler {
	log.SetFormatter(&log.JSONFormatter{})
	return &saramaKafkaProducer{
		brokers: brokers,
		topic:   topic,
	}
}

func (k saramaKafkaProducer) connectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "connectProducer"}).Error(err.Error())
		return nil, err
	}

	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "connectProducer"}).Info("ok")
	return conn, nil
}

func (k saramaKafkaProducer) PushMessage(message []byte) (string, error) {
	producer, err := k.connectProducer(k.brokers)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "PushMessage"}).Error(err.Error())
		return "", err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "PushMessage"}).Error(err.Error())
		return "", err
	}

	log.WithFields(log.Fields{"package": "kafka_handler", "handler": "sarama-producer", "method": "PushMessage"}).Info("ok")
	response := fmt.Sprintf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", k.topic, partition, offset)
	return response, nil
}
