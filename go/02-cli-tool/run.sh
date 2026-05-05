#!/bin/bash

go build -o infra-tool ./starter

if [ "$#" -eq 0 ]; then
  ./infra-tool create-topic --name ads --partitions 3
else
  ./infra-tool "$@"
fi