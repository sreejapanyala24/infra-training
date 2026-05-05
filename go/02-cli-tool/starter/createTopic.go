package main

import (
	"flag"
	"fmt"
)

func createTopic(args []string) {
	fs := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	name := fs.String("name", "", "topic name")
	partitions := fs.Int("partitions", 0, "number of partitions")

	fs.Parse(args)

	if *name == "" {
		fmt.Println("error: topic name required")
		return
	}
	if *partitions <= 0 {
		fmt.Println("error: invalid partitions")
		return
	}

	fmt.Printf("topic=%s partitions=%d status=created\n", *name, *partitions)
	return
}
