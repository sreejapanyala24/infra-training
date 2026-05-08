package main

import "strings"

func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	transient := []string{"timeout", "connection refused", "too many connections", "temporarily unavailable"}
	for _, p := range transient {
		if strings.Contains(errStr, p) {
			return true
		}
	}
	return false
}

func IsTransientKafkaError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	transient := []string{"timeout", "connection refused", "broker unavailable", "temporarily unavailable"}
	for _, p := range transient {
		if strings.Contains(errStr, p) {
			return true
		}
	}
	return false
}
