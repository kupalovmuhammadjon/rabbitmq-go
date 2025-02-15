package main

import (
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

	if err := rabbitMq.PublishMessage("test", "", "Hello world"); err != nil {
		panic(err)
	}

}
