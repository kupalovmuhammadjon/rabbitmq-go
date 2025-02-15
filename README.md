# RabbitMQ Go Client Library

A lightweight, easy-to-use Go client library for interacting with RabbitMQ. This library simplifies common RabbitMQ operations such as publishing messages, consuming queues, and managing connections, allowing you to focus on building your application logic.

---

## What's New

### v1.0.5 (Latest Release)
- **Automatic Reconnection**: The client now automatically detects and recovers from connection failures.
- **Custom Acknowledgments**: Fine-grained control over message acknowledgments for better reliability.
- **Improved Error Handling**: More robust error messages and retry mechanisms.

---

## Features

- **Publish Messages**: Send messages to queues or exchanges with support for JSON, strings, or raw bytes.
- **Consume Messages**: Listen to queues and process incoming messages with a handler function.
- **Queue Management**: Declare queues with configurable parameters (durability, auto-delete, exclusivity, etc.).
- **Automatic Reconnection**: Ensures reliable message delivery in case of connection failures.
- **Custom Acknowledgments**: Provides manual and automatic acknowledgment handling.
- **BOM Handling**: Safely process messages with Byte Order Marks (BOM) removed automatically.
- **Thread-Safe**: Designed for concurrent use in production environments.

---

## Installation

```bash
go get github.com/yourusername/rabbitmq-client
```

---

## Getting Started

### 1. Initialize the Client

```go
package main

import (
	"fmt"
	"github.com/yourusername/rabbitmq-client/rabbitmq"
)

func main() {
	// Replace with your RabbitMQ URL (e.g., "amqp://guest:guest@localhost:5672/")
	url := "amqp://user:password@localhost:5672/"
	client, err := rabbitmq.NewRabbitMQ(url)
	if err != nil {
		panic(err)
	}
	defer client.Close() // Ensure connection is closed gracefully
}
```

---

## Configuration

### Connection URL
Use a valid AMQP URL to connect to your RabbitMQ server:
```go
url := "amqp://user:password@host:port/vhost"
```

### Queue Declaration Parameters
When declaring a queue, configure its behavior:
```go
err := client.DeclareQueue(
	"orders",  // Queue name
	true,      // Durable (survives server restarts)
	false,     // Auto-delete (delete when unused)
	false,     // Exclusive (limited to one connection)
	false,     // No-wait (do not wait for server confirmation)
	nil,       // Additional arguments (e.g., TTL, max length)
)
```

---

## Examples

### Publish a Message
Send messages in various formats (JSON, string, bytes):

```go
// Publish a struct as JSON
type Order struct {
	ID    string  `json:"id"`
	Total float64 `json:"total"`
}
order := Order{ID: "123", Total: 99.99}
err := client.PublishMessage("orders", "", order)

// Publish a string
err := client.PublishMessage("logs", "", "Error: Payment failed")

// Publish raw bytes
err := client.PublishMessage("events", "", []byte("raw_data"))
```

### Consume Messages
Process messages from a queue:

```go
err := client.ConsumeMessages("orders", func(body []byte) {
	// Unmarshal JSON into a struct
	var order Order
	if err := json.Unmarshal(body, &order); err != nil {
		fmt.Printf("Failed to parse message: %v\n", err)
		return
	}
	fmt.Printf("Processing order: %s ($%.2f)\n", order.ID, order.Total)
})
```

---

## Error Handling
Always check for errors after critical operations:
```go
client, err := rabbitmq.NewRabbitMQ(url)
if err != nil {
	panic("Failed to connect to RabbitMQ: " + err.Error())
}

err = client.DeclareQueue("orders", true, false, false, false, nil)
if err != nil {
	panic("Failed to declare queue: " + err.Error())
}
```

---

## Contributing
Contributions are welcome! Follow these steps:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add your feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

---

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## FAQ

### Q: Why am I getting a connection error?
- Ensure your RabbitMQ server is running.
- Verify the connection URL (check credentials, host, port, and vhost).

### Q: How do I handle large messages?
- Use a byte slice or stream data in chunks. Avoid sending very large payloads directly.

### Q: Why are my messages not persisting?
- Declare queues as `durable: true` and publish messages with `Persistent: true` in `amqp.Publishing`.

---

**Happy Coding!** 🚀  
Let us know if you have questions or feedback!