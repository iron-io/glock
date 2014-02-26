package glock

import (
	"bufio"
	"bytes"
	cryptoRand "crypto/rand"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

var glockServers = []string{"localhost:45625", "localhost:45626", "localhost:45627"}

func TestPingPong(t *testing.T) {
	conn, err := net.Dial("tcp", glockServers[0])
	if err != nil {
		t.Error("Unexpected connection error: ", err)
	}
	fmt.Fprintf(conn, "PING\n")
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())
		if split[0] != "PONG" {
			t.Error("Unexpected ping error: ", err)
		} else {
			break
		}
	}
}

func TestLockUnlock(t *testing.T) {
	client1, err := NewClient(glockServers, 10, "", "")
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

func TestLockLimit(t *testing.T) {
	client1, err := NewClient(glockServers, 2000, "", "")
	if err != nil {
		t.Error("Unexpected error creating new client: ", err)
	}
	lockKey := randString(10)
	for i := 0; i < 1000; i++ {
		go client1.Lock(lockKey, 10*time.Second)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Slept for a second, expect to see error below")

	_, err = client1.Lock(lockKey, 10*time.Second)
	fmt.Println("returned from lock", err)
	if err == nil {
		t.Error("Should have rate limited the 1001 connection")
	} else {
		fmt.Println("Got an expected error: ", err)
	}
}

func TestConnectionDrop(t *testing.T) {
	client1, err := NewClient(glockServers, 10, "", "")
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
	client1, err := NewClient(glockServers, 500, "", "")
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
			id1, err := client1.Lock(key, 1000*time.Millisecond)
			if err != nil {
				t.Error("goroutine: ", ii, "Unexpected lock error: ", err)
			}
			fmt.Println("goroutine: ", ii, "GOT LOCK", key)
			time.Sleep(time.Duration(rand.Intn(60)) * time.Millisecond)
			fmt.Println("goroutine: ", ii, "releasing lock", key)
			err = client1.Unlock(key, id1)
			if err != nil {
				fmt.Println("goroutine: ", ii, key, "Already unlocked, it's ok: ", err)
				t.Error("goroutine: ", ii, "Unexpected Unlock error: ", err)
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
	client1, err := NewClient(glockServers, 500, "", "")
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
		id1, err := client1.Lock(key, 1000*time.Millisecond)
		fmt.Println("Returning from lock", id1, err)
		if err != nil {
			t.Error("Unexpected lock error: ", err)
		}
		fmt.Println("GOT LOCK", key)
		time.Sleep(time.Duration(rand.Intn(60)) * time.Millisecond)
		if i == 40 {
			DropServer()
			fmt.Println("Dropping server after lock is acquired")
		}

		fmt.Println("releasing lock", key)
		err = client1.Unlock(key, id1)
		if err != nil {
			fmt.Println(key, "Already unlocked, it's ok: ", err)
			if i != 40 {
				t.Error("goroutine: ", i, "Unexpected Unlock error: ", err)
			} else {
				fmt.Println("server dropped out, safe to ignore")
			}

		} else {
			fmt.Println("released lock", key)
		}
		fmt.Println("pool size: ", client1.Size())

	}
	time.Sleep(1 * time.Second)
	fmt.Println("pool size: ", client1.Size())
}

// A little hack to simulate server dropped out
func DropServer() {
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

func randString(n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	cryptoRand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}
