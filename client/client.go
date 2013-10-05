package glock

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
}

func NewClient(endpoint string) (*Client, error) {
	conn, err := net.Dial("tcp", endpoint)
	reader := bufio.NewReader(conn)
	return &Client{conn, reader}, err
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	fmt.Fprintf(c.conn, "LOCK %s %d \r\n", key, int(duration/time.Millisecond))

	splits, err := c.readResponse()
	if err != nil {
		return id, err
	}

	id, err = strconv.ParseInt(splits[1], 10, 64)
	if err != nil {
		return id, err
	}

	return id, nil
}

func (c *Client) Unlock(key string, id int64) (err error) {
	fmt.Fprintf(c.conn, "UNLOCK %s %d \r\n", key, id)

	splits, err := c.readResponse()
	if err != nil {
		return err
	}

	cmd := splits[0]
	switch cmd {
	case "LOCKED":
		return errors.New("LOCKED")
	case "UNLOCKED":
		return nil
	}
	return errors.New("Unknown reponse format")
}

func (c *Client) Close() error {
	err := c.conn.Close()
	return err
}

func (c *Client) readResponse() (splits []string, err error) {
	response, err := c.reader.ReadString('\n')
	log.Println("glockResponse: ", response)
	if err != nil {
		return nil, err
	}

	trimmedResponse := strings.TrimRight(response, "\r\n")
	splits = strings.Split(trimmedResponse, " ")
	if splits[0] == "ERROR" {
		return nil, errors.New(trimmedResponse)
	}

	return splits, nil
}
