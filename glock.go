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
}

var locksLock sync.RWMutex
var locks = map[string]*timeoutLock{}

func main() {
	var port int
	flag.IntVar(&port, "p", 45625, "port")
	flag.Parse()
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
		go authConn(conn)
	}
}

var (
	unlockedResponse    = []byte("UNLOCKED\n")
	notUnlockedResponse = []byte("NOT_UNLOCKED\n")
	authorizedResponse  = []byte("AUTHORIZED\n")

	errBadFormat      = []byte("ERROR bad command format\n")
	errUnknownCommand = []byte("ERROR unknown command\n")
	errLockNotFound   = []byte("ERROR lock not found\n")
	errUnauthorized   = []byte("ERROR unauthorized\n")
)

func authConn(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())
		cmd := split[0]
		pass := split[1]
		if cmd != "AUTH" {
			conn.Write(errUnauthorized)
			conn.Close()
			return
			if pass != "123" {
				conn.Write(errUnauthorized)
				conn.Close()
				return
			}
		} else {
			conn.Write(authorizedResponse)
			break
		}
	}
	go handleConn(conn)
}

func handleConn(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())
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
			time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
				if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
					lock.mutex.Unlock()
					log.Printf("Timedout: %-12d | Key:  %-15s | Id: %d", timeout, key, id)
				}
			})
			fmt.Fprintf(conn, "LOCKED %v\n", id)

			log.Printf("Request:  %-12s | Key:  %-15s | Timeout: %dms", cmd, key, timeout)
			log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "LOCKED", key, id)

		// UNLOCK <key> <id>
		case "UNLOCK":
			id, err := strconv.ParseInt(split[2], 10, 64)

			if err != nil {
				conn.Write(errBadFormat)
				continue
			}
			locksLock.RLock()
			lock, ok := locks[key]
			locksLock.RUnlock()
			if !ok {
				conn.Write(errLockNotFound)

				log.Printf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				log.Printf("Response: %-12s | Key:  %-15s", "404", key)
				continue
			}
			if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
				lock.mutex.Unlock()
				conn.Write(unlockedResponse)

				log.Printf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "UNLOCKED", key, id)
			} else {
				conn.Write(notUnlockedResponse)

				log.Printf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				log.Printf("Response: %-12s | Key:  %-15s | Id: %d", "NOT_UNLOCKED", key, id)
			}

		default:
			conn.Write(errUnknownCommand)
			continue
		}
	}
}
