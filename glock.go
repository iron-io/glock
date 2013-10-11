package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type timeoutLock struct {
	sync.Mutex
	id int64 // unique ID of the current lock. Only allow an unlock if the correct id is passed
}

var locks = struct {
	sync.RWMutex
	m map[string]*timeoutLock
}{
	m: make(map[string]*timeoutLock),
}

var (
	laddr = flag.String("l", ":45625", "the address to bind to")
)

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", *laddr)
	if err != nil {
		log.Fatalln("error listening", err)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			log.Println("error accepting", err)
			return
		}
		go handleConn(c)
	}
}

var (
	unlockedResponse    = []byte("UNLOCKED\n")
	notUnlockedResponse = []byte("NOT_UNLOCKED\n")

	errBadFormat      = []byte("ERROR bad command format\n")
	errUnknownCommand = []byte("ERROR unknown command\n")
	errLockNotFound   = []byte("ERROR lock not found\n")
)

func handleConn(c net.Conn) {
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())
		if len(split) < 3 {
			c.Write(errBadFormat)
			continue
		}
		cmd := split[0]
		key := split[1]
		switch cmd {
		// LOCK <key> <timeout>
		case "LOCK":
			timeout, err := strconv.Atoi(split[2])

			if err != nil {
				c.Write(errBadFormat)
				continue
			}
			locks.RLock()
			lk, ok := locks.m[key]
			locks.RUnlock()
			if !ok {
				// lock doesn't exist; create it
				locks.Lock()
				lk, ok = locks.m[key]
				if !ok {
					lk = &timeoutLock{}
					locks.m[key] = lk
				}
				locks.Unlock()
			}

			lk.Lock()
			id := atomic.AddInt64(&lk.id, 1)
			time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
				if atomic.CompareAndSwapInt64(&lk.id, id, id+1) {
					lk.Unlock()
					log.Printf("Timedout: %-12d | Key:  %-15s | Id: %d", timeout, key, id)
				}
			})
			fmt.Fprintf(c, "LOCKED %v\n", id)

			log.Printf("Request:  %-12s | Key:  %-15s | Timeout: %dms", cmd, key, timeout)
			log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "LOCKED", key, id)

		// UNLOCK <key> <id>
		case "UNLOCK":
			id, err := strconv.ParseInt(split[2], 10, 64)

			if err != nil {
				c.Write(errBadFormat)
				continue
			}
			locks.RLock()
			lk, ok := locks.m[key]
			locks.RUnlock()
			if !ok {
				c.Write(errLockNotFound)

				log.Printf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				log.Printf("Response: %-12s | Key:  %-15s", "404", key)
				continue
			}
			if atomic.CompareAndSwapInt64(&lk.id, id, id+1) {
				lk.Unlock()
			}
			c.Write(unlockedResponse)

			log.Printf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
			log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "UNLOCKED", key, id)

		default:
			c.Write(errUnknownCommand)
			continue
		}
	}
}
