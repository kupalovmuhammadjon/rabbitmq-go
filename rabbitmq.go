package rabbitmq

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
RabbitMQ is an interface for interacting with RabbitMQ.
It provides methods to publish messages, consume messages, declare queues, and manage connections.
*/
type RabbitMQ interface {
	// PublishMessage publishes a message to a queue or an exchange.
	PublishMessage(queueName, exchangeName string, message interface{}) error

	// ConsumeMessages starts consuming messages from a queue and invokes the provided handler function for each message.
	ConsumeMessages(queueName string, handler func([]byte)) error

	// DeclareQueue declares a RabbitMQ queue with the provided configuration.
	DeclareQueue(queueName string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error

	// Close closes the RabbitMQ connection and channel.
	Close() error
}

// rabbitmq is the concrete implementation of the RabbitMQ interface.
// It maintains the connection and channel to RabbitMQ.
type rabbitmq struct {
	conn    *amqp.Connection // Connection to RabbitMQ.
	channel *amqp.Channel    // Channel for communication with RabbitMQ.
}

// NewRabbitMQ establishes a connection to RabbitMQ and returns a RabbitMQ instance.
// Parameters:
// - url: RabbitMQ connection string.
// Returns:
// - RabbitMQ: An instance of the RabbitMQ interface.
// - error: Error, if any, during connection or channel creation.
func NewRabbitMQ(url string) (RabbitMQ, error) {
	conn, ch, err := connectToRabbitMQ(url)
	if err != nil {
		return nil, err
	}

	return &rabbitmq{
		conn:    conn,
		channel: ch,
	}, nil
}

// connectToRabbitMQ establishes a connection and channel to RabbitMQ.
// Parameters:
// - url: RabbitMQ connection string.
// Returns:
// - *amqp.Connection: RabbitMQ connection.
// - *amqp.Channel: RabbitMQ channel.
// - error: Error, if any, during connection or channel creation.
func connectToRabbitMQ(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

// DeclareQueue declares a RabbitMQ queue with the specified configuration.
// Parameters:
// - queueName: Name of the queue.
// - durable: Whether the queue should survive a RabbitMQ restart.
// - autoDelete: Whether the queue should be deleted when no consumers are connected.
// - exclusive: Whether the queue should be used exclusively by one connection.
// - noWait: Whether the server should wait for a confirmation.
// - args: Additional arguments for the queue.
// Returns:
// - error: Error, if any, during the queue declaration.
func (r *rabbitmq) DeclareQueue(queueName string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error {
	_, err := r.channel.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autoDelete, // auto-delete
		exclusive,  // exclusive
		noWait,     // no-wait
		args,       // arguments
	)
	if err != nil {
		log.Printf("Failed to declare queue %s: %s", queueName, err)
		return err
	}
	log.Printf("Queue declared: %s", queueName)
	return nil
}

// PublishMessage publishes a message to a specific queue or exchange.
// Parameters:
// - queueName: Name of the queue to publish to.
// - exchangeName: Name of the exchange to publish to (use empty string for direct queue publishing).
// - message: Message to be published, in byte format.
// Returns:
// - error: Error, if any, during message publishing.
func (r *rabbitmq) PublishMessage(queueName, exchangeName string, message interface{}) error {
	var body []byte
	var err error

	switch msg := message.(type) {
	case []byte:
		body = msg
	default:
		body, err = json.Marshal(msg)
		if err != nil {
			return err
		}
	}

	// Publish the message to the queue
	err = r.channel.Publish(
		exchangeName, // exchange
		queueName,    // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json", // Set content type to JSON
			Body:        body,               // Message body
		},
	)
	if err != nil {
		log.Printf("Failed to publish message to queue %s: %v", queueName, err)
		return err
	}

	log.Printf("Published message to queue %s: %s", queueName, string(body))
	return nil
}

// ConsumeMessages starts consuming messages from a queue and invokes the provided handler function for each message.
// Parameters:
// - queueName: Name of the queue to consume from.
// - handler: A function to process the message body.
// Returns:
// - error: Error, if any, during message consumption setup.
func (r *rabbitmq) ConsumeMessages(queueName string, handler func([]byte)) error {
	msgs, err := r.channel.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}

	// Start a goroutine to handle incoming messages.
	go func() {
		for msg := range msgs {
			log.Printf("Received message: %s", msg.Body)
			handler(msg.Body) // Call the handler function for each message.
		}
	}()

	return nil
}

// Close closes the RabbitMQ channel and connection.
// Returns:
// - error: Error, if any, during closure of the channel or connection.
func (r *rabbitmq) Close() error {
	// Close the channel.
	if err := r.channel.Close(); err != nil {
		return err
	}

	// Close the connection.
	if err := r.conn.Close(); err != nil {
		return err
	}

	return nil
}
