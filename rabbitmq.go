package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

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
	ConsumeMessages(ctx context.Context, queueName string, handler func([]byte) error) error

	// DeclareQueue declares a RabbitMQ queue with the provided configuration.
	DeclareQueue(queueName string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error

	// Close closes the RabbitMQ connection and channel.
	Close() error
}

/*
rabbitmq is the concrete implementation of the RabbitMQ interface.
It maintains the connection and channel to RabbitMQ.
*/
type rabbitmq struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queues  map[string]amqp.Table
	mu      sync.Mutex
	url     string
}

/*
NewRabbitMQ establishes a connection to RabbitMQ and returns a RabbitMQ instance.

	Parameters:
	- url: RabbitMQ connection string.
	Returns:
	- RabbitMQ: An instance of the RabbitMQ interface.
	- error: Error, if any, during connection or channel creation.
*/
func NewRabbitMQ(url string) (RabbitMQ, error) {
	conn, ch, err := connectToRabbitMQ(url)
	if err != nil {
		return nil, err
	}

	return &rabbitmq{
		conn:    conn,
		channel: ch,
		queues:  make(map[string]amqp.Table),
		url:     url,
	}, nil
}

/*
connectToRabbitMQ establishes a connection and channel to RabbitMQ.

	Parameters:
	- url: RabbitMQ connection string.
	Returns:
	- *amqp.Connection: RabbitMQ connection.
	- *amqp.Channel: RabbitMQ channel.
	- error: Error, if any, during connection or channel creation.
*/
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

/*
DeclareQueue declares a RabbitMQ queue with the specified configuration and stores its details.

	Parameters:
	- queueName: Name of the queue.
	- durable: Whether the queue should survive a RabbitMQ restart.
	- autoDelete: Whether the queue should be deleted when no consumers are connected.
	- exclusive: Whether the queue should be used exclusively by one connection.
	- noWait: Whether the server should wait for a confirmation.
	- args: Additional arguments for the queue.
	Returns:
	- error: Error, if any, during the queue declaration.
*/
func (r *rabbitmq) DeclareQueue(queueName string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, err := r.channel.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return err
	}

	r.queues[queueName] = args
	return nil
}

/*
PublishMessage publishes a message to a specific queue or exchange.

	Parameters:
	- queueName: Name of the queue to publish to.
	- exchangeName: Name of the exchange to publish to (use empty string for direct queue publishing).
	- message: Message to be published, in byte format.
	Returns:
	- error: Error, if any, during message publishing.
*/
func (r *rabbitmq) PublishMessage(queueName, exchangeName string, message interface{}) error {
	var body []byte
	var err error

	switch msg := message.(type) {
	case []byte:
		body = msg
	case string:
		body = []byte(msg)
	default:
		body, err = json.Marshal(msg)
		if err != nil {
			return err
		}
	}

	for attempt := 1; attempt <= 3; attempt++ {
		r.mu.Lock()
		if r.channel == nil {
			r.mu.Unlock()
			return fmt.Errorf("RabbitMQ channel is closed")
		}
		err = r.channel.Publish(exchangeName, queueName, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		r.mu.Unlock()

		if err == nil {
			return nil
		}

		log.Printf("Publish failed (attempt %d): %v", attempt, err)

		if r.isConnectionClosed() {
			log.Println("Reconnecting before retrying publish...")
			r.reconnect()
		}

		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return fmt.Errorf("failed to publish message after retries: %w", err)
}

func (r *rabbitmq) isConnectionClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conn == nil || r.conn.IsClosed() || r.channel == nil
}

/*
ConsumeMessages continuously consumes messages from a queue.

	Parameters:
	- ctx: Context for cancellation.
	- queueName: Name of the queue to consume from.
	- handler: A function to process the message body.
	Returns:
	- error: Error, if the queue is not declared.
*/
func (r *rabbitmq) ConsumeMessages(ctx context.Context, queueName string, handler func([]byte) error) error {
	if _, exists := r.queues[queueName]; !exists {
		return fmt.Errorf("queue %s not declared", queueName)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping consumer for queue: %s", queueName)
			return nil
		default:
			if err := r.consume(queueName, handler); err != nil {
				r.reconnect()
				time.Sleep(1 * time.Second)
			}
		}
	}
}

/*
consume processes messages from a queue.

	Parameters:
	- queueName: Name of the queue to consume from.
	- handler: Function to process each message.
	Returns:
	- error: If consuming fails.
*/
func (r *rabbitmq) consume(queueName string, handler func([]byte) error) error {
	r.mu.Lock()
	msgs, err := r.channel.Consume(queueName, "", false, false, false, false, nil)
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to start consuming messages: %w", err)
	}

	for msg := range msgs {
		body := bytes.TrimPrefix(msg.Body, []byte{0xEF, 0xBB, 0xBF})
		if err := handler(body); err != nil {
			msg.Nack(false, true)
		} else {
			msg.Ack(false)
		}
	}

	return fmt.Errorf("message channel closed, reconnecting")
}

/*
reconnect attempts to re-establish the connection and channel to RabbitMQ.
It also redeclares previously declared queues.
*/
func (r *rabbitmq) reconnect() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for attempts := 1; ; attempts++ {
		log.Println("Attempting to reconnect to RabbitMQ...")
		conn, ch, err := connectToRabbitMQ(r.url)
		if err == nil {
			r.conn = conn
			r.channel = ch
			for queue, args := range r.queues {
				_, _ = r.channel.QueueDeclare(queue, true, false, false, false, args)
			}
			log.Println("Reconnected to RabbitMQ successfully")
			return
		}

		log.Printf("Reconnection attempt %d failed: %v", attempts, err)
		time.Sleep(time.Duration(attempts) * time.Second)
	}
}

/*
Close closes the RabbitMQ connection and channel.
Returns:
- error: If closing fails.
*/
func (r *rabbitmq) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.channel.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}
