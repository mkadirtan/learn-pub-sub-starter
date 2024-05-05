package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		panic(err)
	}

	rmqConnStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rmqConnStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("connection to rabbitmq successful")
	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	ch, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.SimpleQueueTypeTransient)
	if err != nil {
		panic(err)
	}

	gamestate := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.SimpleQueueTypeTransient, handlerPause(gamestate))
	if err != nil {
		panic(err)
	}

	armyMovesQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	armyMovesSubscribeRoutingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyMovesQueueName, armyMovesSubscribeRoutingKey, pubsub.SimpleQueueTypeTransient, handlerMove(gamestate, ch))
	if err != nil {
		panic(err)
	}

	warSubscribeRoutingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, warSubscribeRoutingKey, pubsub.SimpleQueueTypeDurable, handlerWar(gamestate, ch))
	if err != nil {
		panic(err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)
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
			if words[0] == "spawn" {
				fmt.Println("running spawn command...")
				if err = gamestate.CommandSpawn(words); err != nil {
					fmt.Println(err.Error())
				}
			} else if words[0] == "move" {
				fmt.Println("running move command...")
				if armyMove, armyMoveErr := gamestate.CommandMove(words); armyMoveErr != nil {
					fmt.Println(armyMoveErr.Error())
				} else {
					routingKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
					if err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routingKey, armyMove); err != nil {
						fmt.Println(err.Error())
					} else {
						fmt.Println("successfully broadcast move")
					}
				}
			} else if words[0] == "status" {
				fmt.Println("running status command...")
				gamestate.CommandStatus()
			} else if words[0] == "help" {
				gamelogic.PrintClientHelp()
			} else if words[0] == "spam" {
				if len(words) < 2 {
					fmt.Println("you need to provide a number for spam")
					continue
				}

				spamWord := words[1]
				spamInt, parseErr := strconv.ParseInt(spamWord, 10, 64)
				if parseErr != nil {
					fmt.Println("must provide a valid number for spam")
					continue
				}

				routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)

				for i := int64(0); i < spamInt; i++ {
					fmt.Println("spamming...")
					err = pubsub.PublishGob(ch, routing.ExchangePerilTopic, routingKey, routing.GameLog{
						CurrentTime: time.Now(),
						Message:     gamelogic.GetMaliciousLog(),
						Username:    username,
					})
					if err != nil {
						fmt.Println(err.Error())
					}
				}
			} else if words[0] == "quit" {
				gamelogic.PrintQuit()
				break gameloop
			} else {
				fmt.Println("not recognized bro")
			}
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(move gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		if outcome == gamelogic.MoveOutcomeMakeWar {
			routingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername())
			rw := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routingKey, rw); err != nil {
				return pubsub.NackRequeue
			} else {
				return pubsub.Ack
			}
		}

		if outcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(war gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		var publishGob = func(msg string) error {
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     msg,
				Username:    gs.GetUsername(),
			}
			routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, rw.Attacker.Username)
			return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routingKey, gl)
		}

		if outcome == gamelogic.WarOutcomeNotInvolved {
			return pubsub.NackRequeue
		} else if outcome == gamelogic.WarOutcomeNoUnits {
			return pubsub.NackDiscard
		} else if outcome == gamelogic.WarOutcomeOpponentWon || outcome == gamelogic.WarOutcomeYouWon {
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			if err := publishGob(msg); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeDraw {
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			if err := publishGob(msg); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		fmt.Println("outcome not recognized?")
		return pubsub.NackDiscard
	}
}
