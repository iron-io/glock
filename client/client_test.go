package glock

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestLockUnlock(t *testing.T) {
	client1, err := NewClient("localhost:45625", 10)
	if err != nil {
		t.Error("Unexpected new client error: ", err)
	}

	fmt.Println("1 getting lock")
	id1, err := client1.Lock("x", 10*time.Second)
	if err != nil {
		t.Error("Unexpected lock error: ", err)
	}
	fmt.Println("1 got lock")

	go func() {
		fmt.Println("2 getting lock")
		id2, err := client1.Lock("x", 10*time.Second)
		if err != nil {
			t.Error("Unexpected lock error: ", err)
		}
		fmt.Println("2 got lock")

		time.Sleep(1 * time.Second)
		fmt.Println("2 releasing lock")
		err = client1.Unlock("x", id2)
		if err != nil {
			t.Error("Unexpected Unlock error: ", err)
		}
		fmt.Println("2 released lock")
	}()

	fmt.Println("sleeping")
	time.Sleep(2 * time.Second)
	fmt.Println("finished sleeping")

	fmt.Println("1 releasing lock")
	err = client1.Unlock("x", id1)
	if err != nil {
		t.Error("Unexpected Unlock error: ", err)
	}

	fmt.Println("1 released lock")

	time.Sleep(5 * time.Second)
}

func TestConnectionDrop(t *testing.T) {
	client1, err := NewClient("localhost:45625", 10)
	if err != nil {
		t.Error("Unexpected new client error: ", err)
	}

	fmt.Println("closing connection")
	client1.testClose()
	fmt.Println("closed connection")

	fmt.Println("1 getting lock")
	id1, err := client1.Lock("x", 1*time.Second)
	if err != nil {
		t.Error("Unexpected lock error: ", err)
	}
	fmt.Println("1 got lock")

	fmt.Println("1 releasing lock")
	err = client1.Unlock("x", id1)
	if err != nil {
		t.Error("Unexpected Unlock error: ", err)
	}
	fmt.Println("1 released lock")

	client1.testClose()

}

// This is used to simulate dropped out or bad connections in the connection pool
func (c *Client) testClose() {
	size := len(c.connectionPool)
	for x := 0; x < size; x++ {
		connection := <-c.connectionPool
		connection.Close()
		c.connectionPool <- connection
	}
}

func TestConcurrency(t *testing.T) {
	// todo: should document deadlock situation if concurrency in app is more than the number of connections in glock.
	// Or glock should just make new connections if none available. This is probably better way.
	concurrency := 400
	client1, err := NewClient("ec2-54-224-96-21.compute-1.amazonaws.com:45625", concurrency)
	if err != nil {
		t.Error("Unexpected new client error: ", err)
	}
	// map of slices to track who got what and in what order
	orders := make(map[string][]int)
	var wg sync.WaitGroup
	k := 'a'
	for i := 0; i < concurrency; i++ {
		fmt.Println("Value of i is now:", i)
		if i > 0 && i%50 == 0 {
			k += 1
		}
		wg.Add(1)
		go func(ii int, key string) {
			defer wg.Done()
			fmt.Println("goroutine: ", ii, "getting lock", key)
			id1, err := client1.Lock(key, 1000*time.Millisecond)
			if err != nil {
				t.Error("goroutine: ", ii, "Unexpected lock error: ", err)
			}
			fmt.Println("goroutine: ", ii, "GOT LOCK", key)
			orders[key] = append(orders["key"], ii)
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			fmt.Println("goroutine: ", ii, "releasing lock", key)
			err = client1.Unlock(key, id1)
			if err != nil {
				fmt.Println("goroutine: ", ii, key, "Already unlocked, it's ok: ", err)
				//				t.Error("goroutine: ", ii, "Unexpected Unlock error: ", err)
			} else {
				fmt.Println("goroutine: ", ii, "released lock", key)
			}
		}(i, string(k))
	}
	fmt.Println("waiting for waitgroup...")
	wg.Wait()
	fmt.Println("Done waiting for waitgroup")
	fmt.Println("Orders:", orders)

}
