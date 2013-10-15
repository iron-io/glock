package glock

import (
	"bytes"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLockUnlock(t *testing.T) {
	client1, err := NewClient([]string{"localhost:45625", "localhost:45626"}, 10)
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
	client1, err := NewClient([]string{"localhost:45625"}, 10)
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

// // This is used to simulate dropped out or bad connections in the connection pool
func (c *Client) testClose() {
	for server, pool := range c.connectionPools {
		fmt.Println(server)
		size := len(pool)
		for x := 0; x < size; x++ {
			connection := <-pool
			connection.Close()
			pool <- connection
		}
	}
}

func TestConcurrency(t *testing.T) {
	client1, err := NewClient([]string{"localhost:45625", "localhost:45626"}, 500)
	if err != nil {
		t.Error("Unexpected new client error: ", err)
	}

	var wg sync.WaitGroup
	k := 'a'
	for i := 0; i < 1000; i++ {
		fmt.Println("Value of i is now:", i)
		if i > 0 && i%50 == 0 {
			k += 1
		}
		wg.Add(1)
		go func(ii int, key string) {
			defer wg.Done()
			fmt.Println("goroutine: ", ii, "getting lock", key)
			id1, err := client1.Lock(key, 250*time.Millisecond)
			if err != nil {
				t.Error("goroutine: ", ii, "Unexpected lock error: ", err)
			}
			fmt.Println("goroutine: ", ii, "GOT LOCK", key)
			time.Sleep(time.Duration(rand.Intn(60)) * time.Millisecond)
			fmt.Println("goroutine: ", ii, "releasing lock", key)
			err = client1.Unlock(key, id1)
			if err != nil {
				fmt.Println("goroutine: ", ii, key, "Already unlocked, it's ok: ", err)
				//				t.Error("goroutine: ", ii, "Unexpected Unlock error: ", err)
			} else {
				fmt.Println("goroutine: ", ii, "released lock", key)
			}
			fmt.Println("pool size: ", client1.Size())
		}(i, string(k))
	}
	fmt.Println("waiting for waitgroup...")
	wg.Wait()
	fmt.Println("Done waiting for waitgroup")

}

func TestServerDrop(t *testing.T) {
	client1, err := NewClient([]string{"localhost:45625", "localhost:45626"}, 500)
	if err != nil {
		t.Error("Unexpected new client error: ", err)
	}

	k := 'a'
	for i := 0; i < 100; i++ {
		fmt.Println("Value of i is now:", i)
		if i > 0 && i%50 == 0 {
			k += 1
		}
		key := string(k)
		fmt.Println("getting lock", key)
		id1, err := client1.Lock(key, 250*time.Millisecond)
		fmt.Println("Returning from lock", id1, err)
		if err != nil {
			t.Error("Unexpected lock error: ", err)
		}
		fmt.Println("GOT LOCK", key)
		time.Sleep(time.Duration(rand.Intn(60)) * time.Millisecond)
		if i == 40 {
			testDropServer()
		}

		fmt.Println("Dropping server after lock is acquired")
		fmt.Println("releasing lock", key)
		err = client1.Unlock(key, id1)
		if err != nil {
			fmt.Println(key, "Already unlocked, it's ok: ", err)
			//				t.Error("goroutine: ", ii, "Unexpected Unlock error: ", err)
		} else {
			fmt.Println("released lock", key)
		}
		fmt.Println("pool size: ", client1.Size())

	}
	time.Sleep(1 * time.Second)
	fmt.Println("pool size: ", client1.Size())
}

// A little hack to simulate server dropped out
func testDropServer() {
	cmd := exec.Command("pidof", "glock")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		fmt.Println(err)
	}
	pids := strings.Split(out.String(), " ")
	if len(pids) > 1 {
		pid := pids[0]
		cmd = exec.Command("kill", "-9", pid)
		cmd.Run()
	}

}
