package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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

	"github.com/iron-io/common"
	"gopkg.in/inconshreveable/log15.v2"
)

type GlockConfig struct {
	Port           int               `json:"port"`
	LockLimit      int64             `json:"lock_limit"`
	Authentication map[string]string `json:"authentication"`
	Logging        common.LoggingConfig
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
	var env string
	var configFile string
	var logLocal bool
	flag.IntVar(&port, "p", 45625, "port")
	flag.StringVar(&env, "e", "", "Environment")
	flag.StringVar(&configFile, "c", "", "Name of the the file that contains config information")
	flag.BoolVar(&logLocal, "l", false, "Logging to local")
	flag.Parse()

	if configFile != "" {
		LoadConfig(configFile, &config)
	}

	if config.Port == 0 {
		config.Port = port
	}

	if config.Logging.Level == "" {
		config.Logging.Level = "info"
	}

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(config.Port))
	if err != nil {
		log.Fatalln("error listening", err)
	}

	if logLocal {
		config.Logging.To = ""
		config.Logging.Prefix = ""
	}
	common.SetLogging(config.Logging)

	log15.Info("glock server available", "port", config.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log15.Error("error accepting connection", "err", err)
			return
		}
		go authConn(conn)
	}
}

var (
	unlockedResponse    = []byte("UNLOCKED\r\n")
	notUnlockedResponse = []byte("NOT_UNLOCKED\r\n")
	pongResponse        = []byte("PONG\r\n")
	authorizedResponse  = []byte("AUTHORIZED\r\n")

	errBadFormat      = []byte("ERROR 400 bad command format\r\n")
	errUnauthorized   = []byte("ERROR 403 unauthorized\n")
	errLockNotFound   = []byte("ERROR 404 lock not found\r\n")
	errUnknownCommand = []byte("ERROR 405 unknown command\r\n")
	errLockAtCapacity = []byte("ERROR 503 lock at capacity\r\n")
)

func authConn(conn net.Conn) {
	if len(config.Authentication) != 0 {
		authKey, err := randByte(24)
		if err != nil {
			return
		}

		authKeyBase64 := base64.StdEncoding.EncodeToString(authKey)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			split := strings.Fields(scanner.Text())
			cmd := split[0]
			if cmd != "AUTH" || len(split) < 2 {
				log15.Error("unauthorized", "cmd", split)
				unauthorizeConn(conn)
				return
			}

			username := split[1]
			password, ok := config.Authentication[username]
			if !ok {
				log15.Error("unauthorized", "cmd", split)
				unauthorizeConn(conn)
				return
			}

			switch len(split) {
			case 2:
				// Step 1: Return challege
				// cmd: AUTH [username]
				conn.Write([]byte(authKeyBase64 + "\r\n"))
				continue
			case 3:
				// Step 2: Verify challenge
				// cmd: AUTH [username] [expectedMAC]
				expectedMACBase64 := split[2]
				expectedMAC, err := base64.StdEncoding.DecodeString(expectedMACBase64)
				if err != nil {
					log15.Error("unauthorized", "cmd", split)
					unauthorizeConn(conn)
					return
				}

				if CheckMAC([]byte(password), expectedMAC, authKey) {
					log15.Debug("authorized", "cmd", split)
					conn.Write(authorizedResponse)
					break
				}
				fallthrough
			default:
				log15.Error("unauthorized", "cmd", split)
				unauthorizeConn(conn)
				return
			}

		}
	}

	handleConn(conn)
}

func handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		// make sure a panic doesn't take down the whole server
		err := recover()
		if err != nil {
			log15.Error("recovered from panic", "err", err, "stack", string(debug.Stack()))
		}
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())

		if split[0] == "PING" {
			conn.Write(pongResponse)
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
				log15.Error("bad command format", "cmd", split)
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
					log15.Debug("lock timed out", "timeout", timeout, "key", key, "id", id)
				}
			})
			fmt.Fprintf(conn, "LOCKED %v\n", id)

			log15.Debug("locked", "cmd", split, "timeout", timeout, "key", key, "id", id)

		// UNLOCK <key> <id>
		case "UNLOCK":
			id, err := strconv.ParseInt(split[2], 10, 64)

			if err != nil {
				conn.Write(errBadFormat)
				log15.Error("bad command format", "cmd", split)
				continue
			}
			locksLock.RLock()
			lock, ok := locks[key]
			locksLock.RUnlock()
			if !ok {
				conn.Write(errLockNotFound)
				log15.Error("lock not found", "cmd", split, "key", key, "id", id)
				continue
			}
			if atomic.CompareAndSwapInt64(&lock.id, id, id+1) {
				lock.unlockMutex()
				conn.Write(unlockedResponse)
				log15.Debug("unlocked", "cmd", split, "key", key, "id", id)
			} else {
				conn.Write(notUnlockedResponse)
				log15.Debug("not unlocked", "cmd", split, "key", key, "id", id)
			}

		default:
			conn.Write(errUnknownCommand)
			log15.Error(string(errUnknownCommand), ": ", split)
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
	log15.Info("loaded config", "config", config)
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

func randByte(n int) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := rand.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes, err
}

func CheckMAC(password, clientMAC, key []byte) bool {
	mac := hmac.New(sha256.New, key)
	mac.Write(password)
	expectedMAC := mac.Sum(nil)
	return hmac.Equal(clientMAC, expectedMAC)
}

func unauthorizeConn(conn net.Conn) {
	conn.Write(errUnauthorized)
	conn.Close()
}
