package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not open channel: %v", err)
	}
	defer publishCh.Close()

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		if input[0] == "pause" {
			pauseResume(publishCh, true)
		} else if input[0] == "resume" {
			pauseResume(publishCh, false)
		} else if input[0] == "quit" {
			fmt.Println("Stopping the game...")
			break
		} else {
			fmt.Println("Unknown command: " + input[0])
		}
	}

}

func pauseResume(publishCh *amqp.Channel, isPaused bool) {
	if isPaused {
		fmt.Println("Pausing the game...")
	} else {
		fmt.Println("Resuming the game...")
	}
	err := pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: isPaused,
	})
	if err != nil {
		log.Fatalf("could not publish JSON: %v", err)
	}
	if isPaused {
		fmt.Println("Pause message sent!")
	} else {
		fmt.Println("Resume message sent!")
	}

}
