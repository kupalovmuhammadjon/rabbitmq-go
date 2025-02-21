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

	/*
		PublishMessage publishes a message to a specific queue or exchange.

			Parameters:
			- queueName: Name of the queue to publish to.
			- exchangeName: Name of the exchange to publish to (use empty string for direct queue publishing).
			- message: Message to be published, in byte format.
			Returns:
			- error: Error, if any, during message publishing.
	*/
	PublishMessage(queueName, exchangeName string, message interface{}) error

	/*
		ConsumeMessages continuously consumes messages from a queue.

			Parameters:
			- ctx: Context for cancellation.
			- queueName: Name of the queue to consume from.
			- handler: A function to process the message body.
			Returns:
			- error: Error, if the queue is not declared.
	*/
	ConsumeMessages(ctx context.Context, queueName string, prefetch int, handler func([]byte) error) error

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
	DeclareQueue(queueName string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error

	/*
		Close closes the RabbitMQ connection and channel.
		Returns:
		- error: If closing fails.
	*/
	Close() error
}

type rabbitmq struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *amqp.Config
	queues  map[string]queueConfig
	mu      sync.Mutex
	url     string
}

type queueConfig struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  amqp.Table
}

/*
NewRabbitMQ establishes a connection to RabbitMQ and returns a RabbitMQ instance.

	Parameters:
	- url: RabbitMQ connection string.
	Returns:
	- RabbitMQ: An instance of the RabbitMQ interface.
	- error: Error, if any, during connection or channel creation.
*/
func NewRabbitMQ(url string, config *amqp.Config) (RabbitMQ, error) {
	conn, ch, err := connectToRabbitMQ(url, config)
	if err != nil {
		return nil, err
	}

	rmq := &rabbitmq{
		conn:    conn,
		channel: ch,
		config:  config,
		queues:  make(map[string]queueConfig),
		url:     url,
	}

	go rmq.monitorConnection()
	return rmq, nil
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
func connectToRabbitMQ(url string, config *amqp.Config) (*amqp.Connection, *amqp.Channel, error) {
	if config == nil {
		config = &amqp.Config{}
	}

	conn, err := amqp.DialConfig(url, *config)
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

func (r *rabbitmq) DeclareQueue(queueName string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, err := r.channel.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return err
	}

	r.queues[queueName] = queueConfig{
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		NoWait:     noWait,
		Arguments:  args,
	}
	return nil
}

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
ConsumeMessages continuously consumes messages from a queue with a configurable prefetch limit.

	Parameters:
	- ctx: Context for cancellation.
	- queueName: Name of the queue to consume from.
	- prefetch: The number of messages to prefetch (QoS setting) for this consumer.
	- handler: A function to process the message body.
	Returns:
	- error: Error, if the queue is not declared.
*/
func (r *rabbitmq) ConsumeMessages(ctx context.Context, queueName string, prefetch int, handler func([]byte) error) error {
	if _, exists := r.queues[queueName]; !exists {
		return fmt.Errorf("queue %s not declared", queueName)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping consumer for queue: %s", queueName)
			return nil
		default:
			if err := r.consume(queueName, prefetch, handler); err != nil {
				r.reconnect()
				time.Sleep(1 * time.Second)
			}
		}
	}
}

/*
consume processes messages from a queue, applying the QoS settings as specified.

	Parameters:
	- queueName: Name of the queue to consume from.
	- prefetch: The number of messages to prefetch (QoS setting) for this consumer.
	- handler: Function to process each message.
	Returns:
	- error: If consuming fails.
*/
func (r *rabbitmq) consume(queueName string, prefetch int, handler func([]byte) error) error {
	r.mu.Lock()
	// Set QoS: Limit unacknowledged messages to the provided prefetch value for this consumer.
	if err := r.channel.Qos(prefetch, 0, false); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

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
func (r *rabbitmq) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			return err
		}
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
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
		conn, ch, err := connectToRabbitMQ(r.url, r.config)
		if err == nil {
			r.conn = conn
			r.channel = ch
			for queue, cfg := range r.queues {
				_, err = r.channel.QueueDeclare(queue, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, cfg.Arguments)
				if err != nil {
					log.Printf("Failed to redeclare queue %s: %v", queue, err)
				}
			}
			log.Println("Reconnected to RabbitMQ successfully")
			return
		}
		log.Printf("Reconnection attempt %d failed: %v", attempts, err)
		time.Sleep(time.Duration(attempts) * time.Second)
	}
}

// monitorConnection continuously monitors and reconnects if needed.
func (r *rabbitmq) monitorConnection() {
	for {
		if r.isConnectionClosed() {
			log.Println("RabbitMQ connection lost. Attempting to reconnect...")
			r.reconnect()
		}
		time.Sleep(5 * time.Second)
	}
}
