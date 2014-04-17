package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/iron-io/glock/protocol"
	"github.com/iron-io/glock/semaphore"
	"github.com/iron-io/golog"
)

type GlockConfig struct {
	Port           int             `json:"port"`
	LockLimit      int64           `json:"lock_limit"`
	Authentication map[string]bool `json:"authentication"`
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

var (
	config GlockConfig
	glock  *semaphore.Glock

	unlockedResponse    = protocol.Response{Code: 200, Msg: "Unlocked"}
	notUnlockedResponse = protocol.Response{Code: 200, Msg: "Not unlocked"}
	pongResponse        = protocol.Response{Code: 200, Msg: "Pong"}

	errBadFormatResponse      = protocol.Response{Code: 400, Msg: "Invalid parameters for command"}
	errUnauthorizedResponse   = protocol.Response{Code: 401, Msg: "Unauthorized"}
	errLockNotFoundResponse   = protocol.Response{Code: 404, Msg: "Lock not found"}
	errUnknownCommandResponse = protocol.Response{Code: 405, Msg: "Unknown command"}
	errInternalServerResponse = protocol.Response{Code: 500, Msg: "Internal server error"}
	errLockAtCapacityResponse = protocol.Response{Code: 503, Msg: "Lock at capacity"}
)

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

	if logLocal {
		config.Logging.To = ""
		config.Logging.Prefix = ""
	}
	golog.SetLogLevel(config.Logging.Level)
	golog.SetLogLocation(config.Logging.To, config.Logging.Prefix)

	golog.Infoln("Glock Server available at port ", config.Port)

	glock = semaphore.NewGlock()

	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(notCommand)

	r.HandleFunc("/ping", ping).Methods("GET")
	r.HandleFunc("/lock", handle(lock)).Methods("PUT")
	r.HandleFunc("/unlock", handle(unlock)).Methods("PUT")

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil)
}

func handle(hfunc func(http.ResponseWriter, protocol.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		golog.Infoln("Handling new connection", r)

		var request protocol.Request
		err := json.NewDecoder(bufio.NewReader(r.Body)).Decode(&request)
		if err != nil {
			errResponse(w, errBadFormatResponse, err)
			return
		}
		if err := auth(r); err != nil {
			errResponse(w, errUnauthorizedResponse, err)
			return
		}

		hfunc(w, request)
	}
}

// check Authorization header for OAuth token of form:
//  'Authorization: OAuth token_goes_here'
func auth(request *http.Request) error {
	if len(config.Authentication) != 0 {
		auth, ok := request.Header["Authorization"]
		if !ok || len(auth) < 1 {
			err := fmt.Errorf("auth: authentication required but not provided")
			golog.Errorln(err)
			return err
		}

		fields := strings.Fields(auth[0])

		if len(fields) < 2 || fields[0] != "OAuth" { // not really oauth, not sure if that level is necessary
			err := fmt.Errorf("auth: bad auth header: %v", auth)
			golog.Errorln(err)
			return err
		}

		token := fields[1]
		_, ok = config.Authentication[token]

		if !ok {
			err := fmt.Errorf("auth: Token not found: %v", token)
			golog.Errorln(err)
			return err
		}
	}
	return nil
}

func notCommand(w http.ResponseWriter, r *http.Request) {
	errResponse(w, errUnknownCommandResponse, nil)
}

func ping(w http.ResponseWriter, r *http.Request) {
	respond(w, pongResponse)
}

func lock(w http.ResponseWriter, request protocol.Request) {
	if request.Key == "" {
		errResponse(w, errBadFormatResponse, fmt.Errorf("lock command requires a key"))
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
		response := protocol.Response{Code: 201, Msg: "Locked", Id: id}
		respond(w, response)

		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "LOCKED", key, id)
	}
}

func unlock(w http.ResponseWriter, request protocol.Request) {
	if request.Key == "" {
		errResponse(w, errBadFormatResponse, fmt.Errorf("unlock command requires a key"))
		return
	}

	if request.Id == 0 {
		errResponse(w, errBadFormatResponse, fmt.Errorf("unlock command requires an id"))
		return
	}

	key := request.Key
	id := request.Id

	lock, ok := glock.GetLock(key)

	if !ok { // no lock found
		errResponse(w, errLockNotFoundResponse, nil)

		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf(errLockNotFoundResponse.Msg, ": ", request, "| P ", config.Port)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s", config.Port, "404", key)
		return
	}

	// found lock
	if lock.Unlock(id) {
		respond(w, unlockedResponse)
		golog.Debugf("P %-5d | Request:  %+v", config.Port, request)
		golog.Debugf("P %-5d | Response: %-12s | Key:  %-15s | Id: %d", config.Port, "UNLOCKED", key, id)

	} else {
		respond(w, notUnlockedResponse)
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

func errResponse(w http.ResponseWriter, response protocol.Response, err error) {
	if err != nil {
		response.Msg = err.Error()
	}
	respond(w, response)
}

func respond(w http.ResponseWriter, response protocol.Response) {
	b, err := json.Marshal(response)
	if err != nil {
		golog.Errorln("Error marshalling response:", response, err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.Code)
	w.Write(b)
}
