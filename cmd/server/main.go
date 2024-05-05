package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	rmqConnStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rmqConnStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("connection to rabbitmq successful")

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	gamelogic.PrintServerHelp()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

	routingKey := fmt.Sprintf("%s.*", routing.GameLogSlug)
	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routingKey, pubsub.SimpleQueueTypeDurable, handlerLog)
	if err != nil {
		panic(err)
	}

gameloop:
	for {
		select {
		case <-sigchan:
			break
		default:
			words := gamelogic.GetInput()
			if len(words) == 0 {
				continue
			}
			if words[0] == "pause" {
				fmt.Println("sending pause command...")
				msg := routing.PlayingState{
					IsPaused: true,
				}
				if err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, msg); err != nil {
					fmt.Println(err.Error())
					continue
				}
			} else if words[0] == "resume" {
				fmt.Println("sending resume command...")
				msg := routing.PlayingState{
					IsPaused: false,
				}
				if err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, msg); err != nil {
					fmt.Println(err.Error())
					continue
				}
			} else if words[0] == "quit" {
				fmt.Println("exiting...")
				break gameloop
			} else {
				fmt.Println("unrecognized command")
			}
		}
	}
}

func handlerLog(gamelog routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	if err := gamelogic.WriteLog(gamelog); err != nil {
		return pubsub.NackRequeue
	}

	return pubsub.Ack
}
