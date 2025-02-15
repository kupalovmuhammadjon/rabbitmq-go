package main

import (
	"context"
	"fmt"

	"github.com/kupalovmuhammadjon/rabbitmq-go"
)

func main() {
	rabbitMq, err := rabbitmq.NewRabbitMQ("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer rabbitMq.Close()

	if err := rabbitMq.DeclareQueue("test", true, false, false, false, nil); err != nil {
		panic(err)
	}

	if err := rabbitMq.ConsumeMessages(context.Background(), "test", func(body []byte) error {
		fmt.Printf("Received message: %s\n", string(body))
		return nil
	}); err != nil {
		panic(err)
	}

}
