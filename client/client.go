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
	endpoint       string
	conn           net.Conn
	connectionPool chan net.Conn
}

func (c *Client) ClosePool() error {
	size := len(c.connectionPool)
	for x := 0; x < size; x++ {
		conn := <-c.connectionPool
		err := conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func NewClient(endpoint string, size int) (*Client, error) {
	client := &Client{endpoint: endpoint}
	err := client.initPool(size)
	if err != nil {
		return nil, err
	}

	log.Printf("Init with connection pool of %d to Glock server", size)
	return client, nil
}

func (c *Client) initPool(size int) error {
	c.connectionPool = make(chan net.Conn, size)
	for x := 0; x < size; x++ {
		conn, err := net.Dial("tcp", c.endpoint)
		if err != nil {
			return err
		}

		c.connectionPool <- conn
	}
	return nil
}

func (c *Client) getConnection() error {
	if len(c.connectionPool) == 0 {
		conn, err := net.Dial("tcp", c.endpoint)
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}
	conn := <-c.connectionPool
	c.conn = conn
	return nil
}

func (c *Client) releaseConnection() {
	c.connectionPool <- c.conn
	c.conn = nil
}

func (c *Client) redial() error {
	c.conn.Close()
	conn, err := net.Dial("tcp", c.endpoint)
	if err != nil {
		return err
	}
	c.conn = conn

	return nil
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	c.getConnection()
	defer c.releaseConnection()

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
	c.getConnection()
	defer c.releaseConnection()

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

func (c *Client) readResponse() (splits []string, err error) {
	reader := bufio.NewReader(c.conn)
	response, err := reader.ReadString('\n')
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
