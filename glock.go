package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iron-io/golog"
)

type GlockConfig struct {
	Port    int `json:"port"`
	Logging LoggingConfig
}

type LoggingConfig struct {
	To     string `json:"to"`
	Level  string `json:"level"`
	Prefix string `json:"prefix"`
}

type timeoutLock struct {
	mutex sync.Mutex
	id    int64 // unique ID of the current lock. Only allow an unlock if the correct id is passed
}

var locksLock sync.RWMutex
var locks = map[string]*timeoutLock{}

var semaphoresLock sync.RWMutex
var semaphores map[string]Semaphore

var config GlockConfig

func main() {
	var port int
	var configFile string
	flag.IntVar(&port, "p", 45625, "port")
	flag.StringVar(&configFile, "c", "", "Name of the the file that contains config information")
	flag.Parse()

	if configFile != "" {
		LoadConfig(configFile, &config)
	}

	if config.Port == 0 {
		config.Port = port
	}

	if config.Logging.Level == "" {
		config.Logging.Level = "debug"
	}

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(config.Port))
	if err != nil {
		log.Fatalln("error listening", err)
	}

	golog.SetLogLevel(config.Logging.Level)
	golog.SetLogLocation(config.Logging.To, config.Logging.Prefix)

	golog.Infoln("Glock Server available at port ", config.Port)

	semaphores = make(map[string]Semaphore)

	for {
		conn, err := listener.Accept()
		if err != nil {
			golog.Errorln("error accepting", err)
			return
		}
		go handleConn(conn)
	}
}

var (
	unlockedResponse    = []byte("UNLOCKED\r\n")
	notUnlockedResponse = []byte("NOT_UNLOCKED\r\n")
	pongResponse        = []byte("PONG\r\n")

	// semaphore related responses
	pushedResponse  = []byte("PUSHED\r\n")
	fullResponse    = []byte("FULL\r\n")
	poppedResponse  = []byte("POPPED\r\n")
	createdResponse = []byte("CREATED\r\n")

	errBadFormat                 = []byte("ERROR bad command format\r\n")
	errUnknownCommand            = []byte("ERROR unknown command\r\n")
	errLockNotFound              = []byte("ERROR lock not found\r\n")
	errSemaphoreNotFoundResponse = []byte("ERROR semaphore not found\r\n")
)

func handleConn(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())

		if split[0] == "PING" {
			conn.Write(pongResponse)
			golog.Debugf("PING PONG")
			continue
		}

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
				golog.Errorln(string(errBadFormat), ": ", split)
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
					golog.Debugf("P %-5d | Timedout: %-12d | Key:  %-15s | Id: %d", config.Port, timeout, key, id)
				}
			})
			fmt.Fprintf(conn, "LOCKED %v\n", id)

			golog.Debugf("P %-5d | Request:  %-12s | Key:  %-15s | Timeout: %dms", config.Port, cmd, key, timeout)
			golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "LOCKED", key, id)

		// UNLOCK <key> <id>
		case "UNLOCK":
			id, err := strconv.ParseInt(split[2], 10, 64)

			if err != nil {
				conn.Write(errBadFormat)
				golog.Errorln(string(errBadFormat), ": ", split)
				continue
			}
			locksLock.RLock()
			lock, ok := locks[key]
			locksLock.RUnlock()
			if !ok {
				conn.Write(errLockNotFound)

				golog.Debugf("P %-5d | Request:  %-12s | Key:  %-15s | Id: %d", config.Port, cmd, key, id)
				golog.Errorln(string(errLockNotFound), ": ", split, "| P ", config.Port)
				golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s", config.Port, "404", key)
				continue
			}
			if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
				lock.mutex.Unlock()
				conn.Write(unlockedResponse)

				golog.Debugf("P %-5d | Request:  %-12s | Key:  %-15s | Id: %d", config.Port, cmd, key, id)
				golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "UNLOCKED", key, id)
			} else {
				conn.Write(notUnlockedResponse)

				golog.Debugf("P %-5d | Request:  %-12s | Key:  %-15s | Id: %d", config.Port, cmd, key, id)
				golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "NOT_UNLOCKED", key, id)
			}

		case "SET":
			size, err := strconv.Atoi(split[2])
			if err != nil {
				conn.Write(errBadFormat)
				continue
			}

			semaphoresLock.Lock()
			semaphores[key] = NewSemaphore(size)
			semaphoresLock.Unlock()
			conn.Write(createdResponse)

		case "PUSH":
			semaphoresLock.RLock()
			semaphore, ok := semaphores[key]
			semaphoresLock.RUnlock()
			if !ok {
				conn.Write(errSemaphoreNotFoundResponse)
				continue
			}

			if semaphore.Push() {
				conn.Write(pushedResponse)
			} else {
				conn.Write(fullResponse)
			}

		case "POP":
			semaphoresLock.RLock()
			semaphore, ok := semaphores[key]
			semaphoresLock.RUnlock()
			if !ok {
				conn.Write(errSemaphoreNotFoundResponse)
				continue
			}

			semaphore.Pop()
			conn.Write(poppedResponse)
		default:
			conn.Write(errUnknownCommand)
			golog.Errorln(string(errUnknownCommand), ": ", split)
			continue
		}
	}
}

func LoadConfig(configFile string, config interface{}) {
	config_s, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalln("Couldn't find config at:", configFile)
	}

	err = json.Unmarshal(config_s, &config)
	if err != nil {
		log.Fatalln("Couldn't unmarshal config!", err)
	}
	golog.Infoln("config:", config)
}

type Semaphore chan struct{}

func NewSemaphore(size int) Semaphore {
	return make(Semaphore, size)
}

// Non-blocking lock
func (s Semaphore) Push() bool {
	select {
	case s <- struct{}{}:
		return true
	default:
		return false
	}
	return false
}

func (s Semaphore) Pop() {
	<-s
}
