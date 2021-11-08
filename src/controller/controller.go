package controller

import (
	kafka "golang-kafka-producer/src/kafka"

	log "github.com/sirupsen/logrus"
)

//Controller to access the kafka producer and consumer
type Controller interface {
	CommitMessage(message []byte) (interface{}, error)
}

type controller struct {
	kafka_handler kafka.KafkaHandler
}

//NewController initialice a new controller
func NewController(kfk kafka.KafkaHandler) Controller {
	log.SetFormatter(&log.JSONFormatter{})
	return &controller{
		kafka_handler: kfk,
	}
}

//GetAll return all superheroes
func (c *controller) CommitMessage(message []byte) (interface{}, error) {
	err := c.kafka_handler.CommitMessageToQueue(message)
	if err != nil {
		log.WithFields(log.Fields{"package": "controller", "method": "CommitMessage"}).Error(err.Error())
		return nil, err
	}
	log.WithFields(log.Fields{"package": "controller", "method": "CommitMessage"}).Info("ok")

	return nil, nil
}
