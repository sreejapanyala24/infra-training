package main

import (
	"fmt"
	"net/http"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, `{"status":"ok"}`)
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, `{"ready":true}`)
}

func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintln(w, "404 not found")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("method=%s path=%s\n", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", readyHandler)
	mux.HandleFunc("/", notFoundHandler)

	fmt.Println("starting server on :8080")

	err := http.ListenAndServe(":8080", loggingMiddleware(mux))
	if err != nil {
		fmt.Println("error: port already in use")
	}
}
