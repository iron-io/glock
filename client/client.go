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

	"github.com/iron-io/golog"

	"github.com/stathat/consistent"
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
	endpoints       []string
	consistent      *consistent.Consistent
	connectionPools map[string]chan *connection
	password        string
}

type connection struct {
	endpoint string
	conn     net.Conn
	reader   *bufio.Reader
	password string
}

func (c *Client) dial(endpoint string) (net.Conn, error)     { return dial(endpoint, c.password) }
func (c *connection) dial(endpoint string) (net.Conn, error) { return dial(endpoint, c.password) }

func dial(endpoint, password string) (net.Conn, error) {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return conn, err
	}

	_, err = fmt.Fprintf(conn, "AUTH %s\n", password)
	if err != nil {
		return conn, err
	}

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	trimmedResponse := strings.TrimRight(response, "\n")
	splits := strings.Split(trimmedResponse, " ")
	if splits[0] == "ERROR" {
		return nil, errors.New(trimmedResponse)
	}

	log.Println(response)

	return conn, nil
}

// func (c *Client) ClosePool() error {
// 	size := len(c.connectionPool)
// 	for x := 0; x < size; x++ {
// 		connection := <-c.connectionPool
// 		err := connection.Close()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func (c *Client) Size() int {
	var size int
	for _, pool := range c.connectionPools {
		size += len(pool)
	}
	return size
}

func NewClient(endpoints []string, size int, password string) (*Client, error) {
	client := &Client{connectionPools: make(map[string]chan *connection), endpoints: endpoints, password: password}
	client.initServersPool(endpoints)
	client.CheckServerStatus()
	err := client.initPool(size)
	if err != nil {
		golog.Errorln("GlockClient - ", "Initing pool ", err)
		return nil, err
	}

	golog.Debugf("Init with connection pool of %d to Glock server", size)
	return client, nil
}

func (c *Client) initPool(size int) error {
	for _, endpoint := range c.endpoints {
		c.connectionPools[endpoint] = make(chan *connection, size)

		// Init with 1 for now
		for x := 0; x < 1; x++ {
			conn, err := c.dial(endpoint)
			if err != nil {
				golog.Errorln("GlockClient - ", "Removing endpoint from hash table, endpoint: ", endpoint, " error: ", err)
				c.consistent.Remove(endpoint)
				break
			}
			c.connectionPools[endpoint] <- &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: endpoint, password: c.password}
		}
	}
	return nil
}

func (c *Client) getConnection(server, key string) (*connection, error) {
	select {
	case conn := <-c.connectionPools[server]:
		return conn, nil
	default:
		conn, err := c.dial(server)
		if err != nil {
			golog.Errorln("GlockClient - getConnection - ", "Dialing for ", server, err)
			return nil, err
		}
		return &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: server, password: c.password}, nil
	}
}

func (c *Client) releaseConnection(server, key string, connection *connection) {
	select {
	case c.connectionPools[server] <- connection:
	default:
		connection.Close()
	}
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	// its important that we get the server before we do getConnection (instead of inside getConnection) because if that error drops we need to put the connection back to the original mapping.
	server, err := c.consistent.Get(key)
	if err != nil {
		golog.Errorln("GlockClient - ", "Consistent hasing error, key: ", key, " error: ", err)
		return id, err
	}

	connection, err := c.getConnection(server, key)
	if err != nil {
		return id, err
	}
	defer c.releaseConnection(server, key, connection)

	id, err = connection.lock(key, duration)
	if err != nil {
		if err, ok := err.(*glockError); ok {
			if err.errType == connectionErr {
				golog.Errorln("GlockClient - ", "Removing endpoint from hash table, server: ", server, " error: ", err)
				c.consistent.Remove(connection.endpoint)
				// todo for evan/treeder, if it is a connection error remove the failed server and then lock again recursively
				return c.Lock(key, duration)
			} else {
				golog.Errorln("GlockClient - ", "unexpected error: ", err)
				return id, err
			}
		}
	}
	return id, nil
}

func (c *connection) lock(key string, duration time.Duration) (id int64, err error) {
	err = c.fprintf("LOCK %s %d\n", key, int(duration/time.Millisecond))
	if err != nil {
		golog.Errorln("GlockClient - ", "lock error: ", err)
		return id, err
	}

	splits, err := c.readResponse()
	if err != nil {
		golog.Errorln("GlockClient - ", "Lock readResponse error: ", err)
		return id, err
	}

	id, err = strconv.ParseInt(splits[1], 10, 64)
	if err != nil {
		return id, &glockError{errType: internalErr, Err: err}
	}

	return id, nil
}

func (c *Client) Unlock(key string, id int64) (err error) {
	server, err := c.consistent.Get(key)
	if err != nil {
		return err
	}

	connection, err := c.getConnection(server, key)
	if err != nil {
		return err
	}
	defer c.releaseConnection(server, key, connection)

	err = connection.fprintf("UNLOCK %s %d\n", key, id)
	if err != nil {
		golog.Errorln("GlockClient - ", "unlock error: ", err)
		return err
	}

	splits, err := connection.readResponse()
	if err != nil {
		golog.Errorln("GlockClient - ", "unlock readResponse error: ", err)
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
			break
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
		return nil, &glockError{errType: internalErr, Err: errors.New(trimmedResponse)}
	}

	return splits, nil
}

func (c *connection) redial() error {
	c.conn.Close()
	conn, err := c.dial(c.endpoint)
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
