package kafka_handler

type KafkaHandler interface {
	PushMessage(message, key []byte) (string, error)
	CloseProducer() error
}
