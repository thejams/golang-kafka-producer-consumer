package kafka_handler

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// NewConsumer creates a new consumer using the given broker addresses and configuration
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (k kafka) MakeConsumer() {
	worker, err := connectConsumer([]string{k.brokers})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(k.topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	quit := make(chan bool)
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				// TODO, tratar de matar todo el proceso aqui
				// dependiendo si es error de conexion
				fmt.Println(err)
				fmt.Println("ERROR dentro del case error de kafka !!!")
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				return
			}
		}
	}()

	quit <- true
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		fmt.Println("ERROR QUE NO SE COMO PARAR 1!!!")
		panic(err)
	}
}
