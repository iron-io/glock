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

type timeoutLock struct {
	mutex sync.Mutex
	id    int64 // unique ID of the current lock. Only allow an unlock if the correct id is passed
}

var locksLock sync.RWMutex
var locks = map[string]*timeoutLock{}

func main() {
	var port int
	var configFile string
	flag.IntVar(&port, "p", 45625, "port")
	flag.StringVar(&configFile, "config", "", "Name of the the file that contains config information")
	flag.Parse()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalln("error listening", err)
	}

	var config golog.LoggingConfig
	if configFile != "" {
		LoadConfig(configFile, &config)
	}

	golog.SetLogLevel(config.Level)
	golog.SetLogLocation(config.To, config.Prefix)
	golog.Infoln("Glock Server available at port ", port)

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
	unlockedResponse    = []byte("UNLOCKED\n")
	notUnlockedResponse = []byte("NOT_UNLOCKED\n")

	errBadFormat      = []byte("ERROR bad command format\n")
	errUnknownCommand = []byte("ERROR unknown command\n")
	errLockNotFound   = []byte("ERROR lock not found\n")
)

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
					golog.Debugf("Timedout: %-12d | Key:  %-15s | Id: %d", timeout, key, id)
				}
			})
			fmt.Fprintf(conn, "LOCKED %v\n", id)

			golog.Debugf("Request:  %-12s | Key:  %-15s | Timeout: %dms", cmd, key, timeout)
			golog.Debugf("Response: %-12s | Key:  %-15s | Id: %d", "LOCKED", key, id)

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

				golog.Debugf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				golog.Debugf("Response: %-12s | Key:  %-15s", "404", key)
				continue
			}
			if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
				lock.mutex.Unlock()
				conn.Write(unlockedResponse)

				golog.Debugf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				golog.Debugf("Response: %-12s | Key:  %-15s | Id: %d", "UNLOCKED", key, id)
			} else {
				conn.Write(notUnlockedResponse)

				golog.Debugf("Request:  %-12s | Key:  %-15s | Id: %d", cmd, key, id)
				golog.Debugf("Response: %-12s | Key:  %-15s | Id: %d", "NOT_UNLOCKED", key, id)
			}

		default:
			conn.Write(errUnknownCommand)
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
