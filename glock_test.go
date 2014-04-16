package main

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/iron-io/glock/protocol"
)

func newServer() net.Conn {
	client, server := net.Pipe()
	go handleConn(server)
	return client
}

func send(t *testing.T, conn net.Conn, req *protocol.Request) *protocol.Response {
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	err := enc.Encode(&req)
	if err != nil {
		t.Fatal("unexpected encode error:", err)
	}
	var resp protocol.Response
	err = dec.Decode(&resp)
	if err != nil {
		t.Fatal("unexpected decode error:", err)
	}
	return &resp
}

func checkCode(t *testing.T, code, expected int) {
	if code != expected {
		t.Fatalf("expected response %v, got %v", expected, code)
	}
}

func TestLockUnlock(t *testing.T) {
	conn := newServer()
	defer conn.Close()

	resp := send(t, conn, &protocol.Request{Command: "lock", Key: "key", Timeout: 5000})
	checkCode(t, resp.Code, 200)

	resp = send(t, conn, &protocol.Request{Command: "unlock", Key: "key", Id: resp.Id})
	checkCode(t, resp.Code, 200)
}

func TestLockTimeout(t *testing.T) {
	conn := newServer()
	defer conn.Close()

	resp := send(t, conn, &protocol.Request{Command: "lock", Key: "key", Timeout: 500})
	checkCode(t, resp.Code, 200)

	time.Sleep(1 * time.Second)

	resp = send(t, conn, &protocol.Request{Command: "unlock", Key: "key", Id: resp.Id})
	checkCode(t, resp.Code, 204)
}

func TestLockLimit(t *testing.T) {
	oldLimit := config.LockLimit
	config.LockLimit = 1
	defer func() {
		config.LockLimit = oldLimit
	}()

	conn := newServer()
	defer conn.Close()

	resp := send(t, conn, &protocol.Request{Command: "lock", Key: "key", Timeout: 500})
	checkCode(t, resp.Code, 200)

	resp = send(t, conn, &protocol.Request{Command: "lock", Key: "key", Timeout: 500})
	checkCode(t, resp.Code, 503)
}
