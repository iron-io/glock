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
	"sync"

	"io"

	"github.com/iron-io/glock/protocol"
	"github.com/iron-io/glock/semaphore"
	"github.com/iron-io/golog"
)

type GlockConfig struct {
	Port           int               `json:"port"`
	LockLimit      int64             `json:"lock_limit"`
	Authentication map[string]string `json:"authentication"`
	Logging        LoggingConfig
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

var config GlockConfig
var glock *semaphore.Glock

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
		config.Logging.Level = "debug"
	}

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(config.Port))
	if err != nil {
		log.Fatalln("error listening", err)
	}

	if logLocal {
		config.Logging.To = ""
		config.Logging.Prefix = ""
	}
	golog.SetLogLevel(config.Logging.Level)
	golog.SetLogLocation(config.Logging.To, config.Logging.Prefix)

	golog.Infoln("Glock Server available at port ", config.Port)

	glock = semaphore.NewGlock()

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
	unlockedResponse    = protocol.Response{Code: 200, Msg: "Unlocked"}
	notUnlockedResponse = protocol.Response{Code: 204, Msg: "Not unlocked"}
	pongResponse        = protocol.Response{Code: 200, Msg: "Pong"}
	authorizedResponse  = protocol.Response{Code: 200, Msg: "Authorized"}

	errBadFormatResponse      = protocol.Response{Code: 400, Msg: "Invalid parameters for command"}
	errUnauthorizedResponse   = protocol.Response{Code: 403, Msg: "Unauthorized"}
	errLockNotFoundResponse   = protocol.Response{Code: 404, Msg: "Lock not found"}
	errUnknownCommandResponse = protocol.Response{Code: 405, Msg: "Unknown command"}
	errInternalServerResponse = protocol.Response{Code: 500, Msg: "Internal server error"}
	errLockAtCapacityResponse = protocol.Response{Code: 503, Msg: "Lock at capacity"}
)

func authenticate(request protocol.Request) error {
	if len(config.Authentication) != 0 {
		password, ok := config.Authentication[request.Username]
		if !ok {
			err := fmt.Errorf("auth: User not found: %v", request.Username)
			golog.Errorln(err)
			return err
		}

		if password != request.Token {
			err := fmt.Errorf("auth: Bad token for %v", request.Username)
			golog.Errorln(err)
			return err
		}
	}
	return nil
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

	authenticated := false
	if len(config.Authentication) == 0 {
		authenticated = true
	}
	golog.Infoln("Handling new connection", conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	for {
		var request protocol.Request
		if err := dec.Decode(&request); err == io.EOF {
			golog.Debugf("Got an EOF, breaking and closing this connection.")
			break
		} else if err != nil {
			errResponse(conn, errBadFormatResponse, err)
			continue
		}

		switch request.Command {
		case "ping":
			respond(conn, pongResponse)
			golog.Debugf("PING PONG")
			continue

		case "auth":
			err := authenticate(request)
			if err != nil {
				errResponse(conn, errUnauthorizedResponse, err)
				// todo: close connection and perhaps ban IP for a bit if too many failed auths.
				continue
			}
			authenticated = true
			respond(conn, authorizedResponse)
			continue

		default:
			if !authenticated {
				// todo: ban IP for a bit if too many failed auths
				errResponse(conn, errUnauthorizedResponse, nil)
				continue
			}
			switch request.Command {
			case "lock":
				lock(conn, request)
			case "unlock":
				unlock(conn, request)

			default:
				errResponse(conn, errUnknownCommandResponse, nil)
				continue
			}

		}
	}
}

func lock(conn net.Conn, request protocol.Request) {
	if request.Key == "" {
		errResponse(conn, errBadFormatResponse, fmt.Errorf("lock command requires a key"))
		return
	}
	key := request.Key
	timeout := request.Timeout
	size := request.Size
	if size == 0 {
		size = 1
	}

	lock := glock.GetOrCreateLock(key, 1)
	id := lock.BLock(timeout)
	if id == 0 {
		// id should not be zero
	} else {
		response := protocol.Response{Code: 200, Msg: "Locked", Id: id}
		respond(conn, response)

		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "LOCKED", key, id)
	}
}

func unlock(conn net.Conn, request protocol.Request) {
	if request.Key == "" {
		errResponse(conn, errBadFormatResponse, fmt.Errorf("unlock command requires a key"))
		return
	}

	if request.Id == 0 {
		errResponse(conn, errBadFormatResponse, fmt.Errorf("unlock command requires an id"))
		return
	}

	key := request.Key
	id := request.Id

	lock, ok := glock.GetLock(key)

	if !ok { // no lock found
		errResponse(conn, errLockNotFoundResponse, nil)

		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf(errLockNotFoundResponse.Msg, ": ", request, "| P ", config.Port)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s", config.Port, "404", key)
		return
	}

	// found lock
	if lock.Unlock(id) {
		respond(conn, unlockedResponse)
		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "UNLOCKED", key, id)

	} else {
		respond(conn, notUnlockedResponse)
		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "NOT_UNLOCKED", key, id)
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

func errResponse(conn net.Conn, response protocol.Response, err error) {
	if err != nil {
		response.Msg = err.Error()
	}
	respond(conn, response)
}

func respond(conn net.Conn, response protocol.Response) {
	b, err := json.Marshal(response)
	if err != nil {
		golog.Errorln("Error marshalling response:", response, err)
	}
	conn.Write(b)
}
