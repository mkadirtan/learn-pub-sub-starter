package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

var (
	SimpleQueueTypeDurable   SimpleQueueType = 0
	SimpleQueueTypeTransient SimpleQueueType = 1
)

type AckType int

var (
	Ack         AckType = 0
	NackRequeue AckType = 1
	NackDiscard AckType = 2
)

func PublishGob[T any](ch *amqp.Channel, exchange string, routingKey string, val T) error {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(val); err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        data.Bytes(),
	})
}

func SubscribeGob[T any](conn *amqp.Connection, exchange string, queueName string, key string, simpleQueueType SimpleQueueType, handler func(T) AckType) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return err
	}
	chDelivery, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range chDelivery {
			data := bytes.NewBuffer(delivery.Body)
			var t T
			err = gob.NewDecoder(data).Decode(&t)
			if err != nil {
				fmt.Println(err.Error())
				delivery.Nack(false, false)
				continue
			}

			result := handler(t)
			switch result {
			case Ack:
				_ = delivery.Ack(false)
			case NackRequeue:
				_ = delivery.Nack(false, true)
			case NackDiscard:
				_ = delivery.Nack(false, false)
			}
		}
	}()

	return nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange string, routingKey string, val T) error {
	jsonB, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonB,
	})
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange string, queueName string, key string, simpleQueueType SimpleQueueType, handler func(T) AckType) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	chDelivery, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range chDelivery {
			var t T
			err = json.Unmarshal(delivery.Body, &t)
			if err != nil {
				fmt.Println(err.Error())
				delivery.Ack(false)
				continue
			}

			result := handler(t)
			switch result {
			case Ack:
				_ = delivery.Ack(false)
			case NackRequeue:
				_ = delivery.Nack(false, true)
			case NackDiscard:
				_ = delivery.Nack(false, false)
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	// defer ch.Close()

	var durable, autoDelete, exclusive, noWait bool

	if simpleQueueType == SimpleQueueTypeDurable {
		durable = true
	}
	if simpleQueueType == SimpleQueueTypeTransient {
		autoDelete = true
		exclusive = true
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}
