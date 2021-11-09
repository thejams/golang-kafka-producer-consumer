package kafka_handler

type KafkaHandler interface {
	PushMessage(message []byte) (string, error)
}
