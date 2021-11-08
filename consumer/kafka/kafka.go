package kafka_handler

import log "github.com/sirupsen/logrus"

type KafkaHandler interface {
	MakeConsumer()
}

type kafka struct {
	brokers string
	topic   string
}

//NewKafkaHandler initialice a new kafka handler
func NewKafkaHandler(brokers string, topic string) KafkaHandler {
	log.SetFormatter(&log.JSONFormatter{})
	return &kafka{
		brokers: brokers,
		topic:   topic,
	}
}
