package kafka_handler

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func (k kafka) connectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (k kafka) CommitMessageToQueue(message []byte) (string, error) {
	brokersUrl := []string{}
	brokersUrl = append(brokersUrl, k.brokers)

	producer, err := k.connectProducer(brokersUrl)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	response := fmt.Sprintf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", k.topic, partition, offset)
	return response, nil
}
