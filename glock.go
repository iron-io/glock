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
	mutex sync.Mutex
	id    int64 // unique ID of the current lock. Only allow an unlock if the correct id is passed
	timer *time.Timer
}

func (l *timeoutLock) Unlock() {
	l.timer.Stop()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered after bad unlock", r)
		}
	}()
	l.mutex.Unlock()
}

var locksLock sync.RWMutex
var locks = map[string]*timeoutLock{}

func main() {
	var port int
	flag.IntVar(&port, "p", 45625, "port")
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalln("error listening", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("error accepting", err)
			return
		}
		go handleConn(conn)
	}
}

var (
	unlockedResponse    = []byte("UNLOCKED\n")
	notUnlockedResponse = []byte("NOT_UNLOCKED\n")

	errBadFormat      = []byte("ERROR bad command format\n")
	errUnknownCommand = []byte("ERROR unknown command\n")
	errLockNotFound   = []byte("ERROR lock not found\n")
)

func handleConn(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		log.Println("IN:", line)
		split := strings.Fields(line)
		if len(split) < 3 {
			conn.Write(errBadFormat)
			continue
		}
		cmd := split[0]
		key := split[1]
		switch cmd {
		// LOCK <key> <timeout>
		case "LOCK":
			timeout, err := strconv.Atoi(split[2])
			if err != nil {
				conn.Write(errBadFormat)
				continue
			}
			locksLock.RLock()
			lock, ok := locks[key]
			locksLock.RUnlock()
			if !ok {
				// lock doesn't exist; create it
				locksLock.Lock()
				lock, ok = locks[key]
				if !ok {
					lock = &timeoutLock{}
					locks[key] = lock
				}
				locksLock.Unlock()
			}

			lock.mutex.Lock()
			id := atomic.AddInt64(&lock.id, 1)
			lock.timer = time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
				log.Printf("Timedout: %-12d | Key: %-15s | Id: %d", timeout, key, id)
				lock.Unlock()
			})
			fmt.Fprintf(conn, "LOCKED %v\n", id)

			log.Printf("Request:  %-12s | Key:  %-15s | Timeout: %dms", cmd, key, timeout)
			log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "LOCKED", key, id)

		// UNLOCK <key> <id>
		case "UNLOCK":
			id, err := strconv.ParseInt(split[2], 10, 64)
			log.Printf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
			if err != nil {
				conn.Write(errBadFormat)
				continue
			}
			locksLock.RLock()
			lock, ok := locks[key]
			locksLock.RUnlock()
			if !ok {
				conn.Write(errLockNotFound)
				log.Printf("Response: %-12s | Key:  %-15s", "404", key)
				continue
			}
			lock.Unlock()
			conn.Write(unlockedResponse)
			log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "UNLOCKED", key, id)

		default:
			conn.Write(errUnknownCommand)
			continue
		}
	}
}
