package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("error: command required")
		return
	}
	command := os.Args[1]

	switch command {
	case "create-topic":
		createTopic(os.Args[2:])
	case "delete-topic":
		deleteTopic(os.Args[2:])
	default:
		fmt.Println("error: unknown command")
	}
}
