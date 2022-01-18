package kafka_handler

type KafkaHandler interface {
	ConsumeMessage()
	CloseConsumer() error
}
