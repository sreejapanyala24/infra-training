package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type config struct {
	Service string `json:"service"`
	Env     string `json:"env"`
	Region  string `json:"region"`
}

func main() {
	filename := os.Args[1]
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("error: file not found")
			return
		}
		fmt.Println("error opening file")
		return
	}
	defer file.Close()
	var cfg config
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&cfg)
	if err != nil {
		fmt.Println("error: invalid config")
		return
	}
	fmt.Printf("service=%s env=%s region=%s status=started\n", cfg.Service, cfg.Env, cfg.Region)
}
