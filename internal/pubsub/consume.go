package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackDiscard
	NackRequeue
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	queue, err := ch.QueueDeclare(queueName,
		queueType == SimpleQueueDurable,
		queueType == SimpleQueueTransient,
		queueType == SimpleQueueTransient,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		})
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	delivery, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range delivery {
			var dat T
			err = json.Unmarshal(d.Body, &dat)
			if err != nil {
				fmt.Println(err)
				continue
			}
			ack := handler(dat)
			switch ack {
			case Ack:
				err = d.Ack(false)
				if err != nil {
					fmt.Println(err)
					continue
				}
				log.Println("Ack")
			case NackRequeue:
				err = d.Nack(false, true)
				if err != nil {
					fmt.Println(err)
					continue
				}
				log.Println("NackRequeue")
			case NackDiscard:
				err = d.Nack(false, false)
				if err != nil {
					fmt.Println(err)
					continue
				}
				log.Println("NackDiscard")
			}

		}
	}()

	return nil
}
