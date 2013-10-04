package glock

import (
	"fmt"
	"testing"
	"time"
)

func TestLockUnlock(t *testing.T) {
	client := NewClient("localhost:45625")

	fmt.Println("1 getting lock")
	id1 := client.LockEx("x", 10000)
	fmt.Println("1 got lock")

	go func() {

		fmt.Println("2 getting lock")
		id2 := client.LockEx("x", 10000)
		fmt.Println("2 got lock")

		fmt.Println("2 releasing lock")
		client.UnLock("x", id2)
		fmt.Println("2 released lock")

	}()

	fmt.Println("sleeping")
	time.Sleep(5 * time.Second)
	fmt.Println("finished sleeping")

	fmt.Println("1 releasing lock")
	client.UnLock("x", id1)
	fmt.Println("1 released lock")

	time.Sleep(5 * time.Second)
}
