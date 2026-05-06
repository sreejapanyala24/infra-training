package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(jobs <-chan int, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("job=%d done\n", job)

		results <- job
	}
}

func main() {
	var wg sync.WaitGroup

	jobs := make(chan int)
	results := make(chan int)

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)

	}
	go func() {
		for j := 1; j <= 100; j++ {
			jobs <- j
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	completed := 0

	for range results {
		completed++
	}

	fmt.Printf("completed jobs=%d\n", completed)
}
