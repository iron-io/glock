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
	endpoint string
	conn     net.Conn
	reader   *bufio.Reader
}

func NewClient(endpoint string) (*Client, error) {
	conn, err := net.Dial("tcp", endpoint)
	reader := bufio.NewReader(conn)
	return &Client{endpoint, conn, reader}, err
}

func (c *Client) redial() error {
	c.Close()
	conn, err := net.Dial("tcp", c.endpoint)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)

	return nil
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	err = c.fprintf("LOCK %s %d \r\n", key, int(duration/time.Millisecond))
	if err != nil {
		return id, err
	}

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
	err = c.fprintf("UNLOCK %s %d \r\n", key, id)
	if err != nil {
		return err
	}

	splits, err := c.readResponse()
	if err != nil {
		return err
	}

	cmd := splits[0]
	switch cmd {
	case "NOT_UNLOCKED":
		return errors.New("NOT_UNLOCKED")
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

func (c *Client) fprintf(format string, a ...interface{}) error {
	for i := 0; i < 3; i++ {
		_, err := fmt.Fprintf(c.conn, format, a...)
		if err != nil {
			err = c.redial()
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}
	return nil
}
