package kafka_handler

type KafkaHandler interface {
	ConsumeMessage() (string, error)
}
