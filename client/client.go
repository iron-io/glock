package glock

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	conn net.Conn
}

func NewClient(endpoint string) (*Client, error) {
	conn, err := net.Dial("tcp", endpoint)
	return &Client{conn}, err
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	fmt.Fprintf(c.conn, "LOCK %s %d \r\n", key, int(duration/time.Millisecond))

	splits, err := readResponse(c.conn)
	if err != nil {
		return id, err
	}

	id, err = strconv.ParseInt(splits[1], 10, 64)
	if err != nil {
		return id, err
	}

	return id, nil
}

func (c *Client) Unlock(key string, id int64) (ok bool, err error) {
	fmt.Fprintf(c.conn, "UNLOCK %s %d \r\n", key, id)

	splits, err := readResponse(c.conn)
	if err != nil {
		return false, err
	}

	cmd := splits[0]
	switch cmd {
	case "LOCKED":
		ok = false
	case "UNLOCKED":
		ok = true
	}

	return ok, nil
}

func (c *Client) Close() error {
	err := c.conn.Close()
	return err
}

func readResponse(conn net.Conn) (splits []string, err error) {
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return []string{}, err
	}

	trimmedResponse := strings.TrimRight(response, "\r\n")
	splits = strings.Split(trimmedResponse, " ")
	if splits[0] == "ERROR" {
		return []string{}, errors.New(trimmedResponse)
	}

	return splits, nil
}
