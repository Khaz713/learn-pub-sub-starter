package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshaled, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("error marshalling value to JSON: %v", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        marshaled,
	})
	if err != nil {
		return fmt.Errorf("error publishing value to JSON: %v", err)
	}
	return nil

}
