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

type glockError struct {
	errType string
	Err     error
}

func (e *glockError) Error() string {
	return e.Err.Error()
}

var (
	connectionErr string = "Connection Error"
	internalErr   string = "Internal Error"
)

type Client struct {
	endpoint       string
	connectionPool chan *connection
}

type connection struct {
	endpoint string
	conn     net.Conn
	reader   *bufio.Reader
}

func (c *Client) ClosePool() error {
	size := len(c.connectionPool)
	for x := 0; x < size; x++ {
		connection := <-c.connectionPool
		err := connection.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Size() int {
	return len(c.connectionPool)
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
	c.connectionPool = make(chan *connection, size)
	for x := 0; x < size; x++ {
		conn, err := net.Dial("tcp", c.endpoint)
		if err != nil {
			return err
		}
		c.connectionPool <- &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: c.endpoint}
	}
	return nil
}

func (c *Client) getConnection() (*connection, error) {
	return <-c.connectionPool, nil
}

func (c *Client) releaseConnection(connection *connection) {
	c.connectionPool <- connection
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	connection, err := c.getConnection()
	if err != nil {
		return id, err
	}
	defer c.releaseConnection(connection)

	err = connection.fprintf("LOCK %s %d\n", key, int(duration/time.Millisecond))
	if err != nil {
		return id, err
	}

	splits, err := connection.readResponse()
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
	connection, err := c.getConnection()
	if err != nil {
		return err
	}
	defer c.releaseConnection(connection)

	err = connection.fprintf("UNLOCK %s %d\n", key, id)
	if err != nil {
		return err
	}

	splits, err := connection.readResponse()
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

func (c *connection) fprintf(format string, a ...interface{}) error {
	for i := 0; i < 3; i++ {
		_, err := fmt.Fprintf(c.conn, format, a...)
		if err != nil {
			err = c.redial()
			if err != nil {
				return &glockError{errType: connectionErr, Err: err}
			}
		} else {
			return nil
		}
	}
	return nil
}

func (c *connection) readResponse() (splits []string, err error) {
	response, err := c.reader.ReadString('\n')
	log.Println("glockResponse: ", response)
	if err != nil {
		return nil, &glockError{errType: connectionErr, Err: err}
	}

	trimmedResponse := strings.TrimRight(response, "\n")
	splits = strings.Split(trimmedResponse, " ")
	if splits[0] == "ERROR" {
		return nil, &glockError{errType: internalErr, Err: trimmedResponse}
	}

	return splits, nil
}

func (c *connection) redial() error {
	c.conn.Close()
	conn, err := net.Dial("tcp", c.endpoint)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)

	return nil
}

func (c *connection) Close() error {
	c.reader = nil
	return c.conn.Close()
}
