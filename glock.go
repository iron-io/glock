package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iron-io/golog"
)

type GlockConfig struct {
	Port      int `json:"port"`
	Logging   LoggingConfig
	LockLimit int64 `json:"lock_limit"`
}

type LoggingConfig struct {
	To     string `json:"to"`
	Level  string `json:"level"`
	Prefix string `json:"prefix"`
}

type timeoutLock struct {
	mutex     sync.Mutex
	id        int64 // unique ID of the current lock. Only allow an unlock if the correct id is passed
	lockCount int64
}

var locksLock sync.RWMutex
var locks = map[string]*timeoutLock{}
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

	errBadFormat      = []byte("ERROR 400 bad command format\r\n")
	errUnknownCommand = []byte("ERROR 405 unknown command\r\n")
	errLockNotFound   = []byte("ERROR 404 lock not found\r\n")
	errLockAtCapacity = []byte("ERROR 503 lock at capacity\r\n")
)

func handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		// make sure a panic doesn't take down the whole server
		err := recover()
		if err != nil {
			golog.Errorf("recovered from panic: %v\n%s\n", err, debug.Stack())
		}
	}()

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

			if !lock.lockMutex() {
				conn.Write(errLockAtCapacity)
				continue
			}
			id := atomic.AddInt64(&lock.id, 1)
			time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
				if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
					lock.unlockMutex()
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
				lock.unlockMutex()
				conn.Write(unlockedResponse)

				golog.Debugf("P %-5d | Request:  %-12s | Key:  %-15s | Id: %d", config.Port, cmd, key, id)
				golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "UNLOCKED", key, id)
			} else {
				conn.Write(notUnlockedResponse)

				golog.Debugf("P %-5d | Request:  %-12s | Key:  %-15s | Id: %d", config.Port, cmd, key, id)
				golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "NOT_UNLOCKED", key, id)
			}

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

func (l *timeoutLock) lockMutex() bool {
	if config.LockLimit != 0 {
		for {
			count := atomic.LoadInt64(&l.lockCount)
			if count >= config.LockLimit {
				return false
			}

			if atomic.CompareAndSwapInt64(&l.lockCount, count, count+1) {
				break
			}
		}
	}
	l.mutex.Lock()
	return true
}

func (l *timeoutLock) unlockMutex() {
	l.mutex.Unlock()
	if config.LockLimit != 0 {
		atomic.AddInt64(&l.lockCount, -1)
	}
}
