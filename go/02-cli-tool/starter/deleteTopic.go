package main

import (
	"flag"
	"fmt"
)

func deleteTopic(args []string) {
	fs := flag.NewFlagSet("delete-topic", flag.ContinueOnError)
	name := fs.String("name", "", "topic name")

	fs.Parse(args)

	if *name == "" {
		fmt.Println("error: topic name required")
		return
	}

	fmt.Printf("topic=%s status=deleted\n", *name)
	return
}
