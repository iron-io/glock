package protocol

import (
	"fmt"
)

// Incoming requests
type Request struct {
	Command  string
	Username string // not sure if we need a username?  A global token might be fine
	Token    string // for authentication, set in config
	Key      string
	Size     int // size of the semaphore
	Timeout  int
	Id       int64
}

// Outgoing responses
type Response struct {
	Code int
	Msg  string
	Id   int64
}

func (r *Response) Error() string {
	return fmt.Sprintf("%v: %v", r.Code, r.Msg)
}
