package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kupalovmuhammadjon/rabbitmq-go"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitMq, err := rabbitmq.NewRabbitMQ("amqp://guest:guest@localhost:5672/", &amqp091.Config{
		Heartbeat: 5 * time.Minute})
	if err != nil {
		panic(err)
	}
	defer rabbitMq.Close()

	if err := rabbitMq.DeclareQueue("test", true, false, false, false, nil); err != nil {
		panic(err)
	}

	if err := rabbitMq.ConsumeMessages(context.Background(), "test", 3, func(body []byte) error {
		fmt.Printf("Received message: %s\n", string(body))
		time.Sleep(5 * time.Second)
		return nil
	}); err != nil {
		panic(err)
	}

}
