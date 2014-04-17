package protocol

// Incoming requests
type Request struct {
	Key     string `json:"key"`
	Size    int    `json:"size"` // size of the semaphore
	Timeout int    `json:"timeout"`
	Id      int64  `json:"id"`
}

// Outgoing responses
type Response struct {
	Code int    `json:"-"`
	Msg  string `json:"msg"`
	Id   int64  `json:"id,omitempty"`
}
