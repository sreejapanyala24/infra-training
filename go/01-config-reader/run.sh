#!/bin/bash

cd starter || exit 1

# Use argument if provided, otherwise default to config.json
FILE=${1:-config.json}

go run main.go "$FILE"