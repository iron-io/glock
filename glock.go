package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

	errBadFormat      = []byte("ERROR bad command format\r\n")
	errUnknownCommand = []byte("ERROR unknown command\r\n")
	errLockNotFound   = []byte("ERROR lock not found\r\n")
)

type command struct {
	handler func(args []string) []byte
	args    []string
}

var commands = map[string]command{
	"PING":   {ping, []string{}},
	"LOCK":   {lock, []string{"Key", "T/O"}},
	"UNLOCK": {unlock, []string{"Key", "Id"}},
}

func glog(logType string, requestId int, comm string) {
	golog.Debugf("%-5d %5d %-8s: %-25s", config.Port, requestId, logType, comm)
}

func (c command) logRequest(requestId int, split []string) {
	var comm string
	for index, arg := range c.args {
		comm += fmt.Sprint(arg, ": ", split[index], " | ")
	}
	glog("Request", requestId, comm)
}

func increment(requestId int) int {
	if requestId > 99999 {
		requestId = 1
	} else {
		requestId += 1
	}
	return requestId
}

func handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		// make sure a panic doesn't take down the whole server
		err := recover()
		if err != nil {
			golog.Errorf("recovered from panic: %v\n%s\n", err, debug.Stack())
		}
	}()

	var requestId int
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())
		cmd, ok := commands[split[0]]
		if !ok {
			conn.Write(errUnknownCommand)
			golog.Errorln(string(errUnknownCommand), ": ", split)
			continue
		}

		requestId = increment(requestId)
		cmd.logRequest(requestId, split[1:])

		if len(split)-1 != len(cmd.args) {
			conn.Write(errBadFormat)
			golog.Errorln(string(errBadFormat), ": ", split)
			continue
		}

		resp := cmd.handler(split[1:])
		glog("Response", requestId, string(resp))

		_, err := conn.Write(resp)
		if err != nil {
			if err != io.EOF {
				golog.Errorln("error writing response:", err)
			}
			break
		}
	}
}

// PING
func ping([]string) []byte {
	return pongResponse
}

// LOCK <key> <timeout>
func lock(args []string) []byte {
	key := args[0]
	timeout, err := strconv.Atoi(args[1])

	if err != nil {
		golog.Errorln(string(errBadFormat), ": ", args)
		return errBadFormat
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

	resp := make([]byte, 0, len("LOCKED \r\n")+10)
	resp = append(resp, "LOCKED "...)
	resp = strconv.AppendInt(resp, id, 10)
	resp = append(resp, "\r\n"...)
	return resp
}

// UNLOCK <key> <id>
func unlock(args []string) []byte {
	key := args[0]

	id, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		golog.Errorln(string(errBadFormat), ": ", args)
		return errBadFormat
	}

	locksLock.RLock()
	lock, ok := locks[key]
	locksLock.RUnlock()
	if !ok {
		return errLockNotFound
	}
	if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
		lock.mutex.Unlock()
		return unlockedResponse
	}
	return notUnlockedResponse
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
