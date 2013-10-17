package glock

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
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
	poolsLock       sync.RWMutex
	connectionPools map[string]chan *connection
	poolSize        int
}

type connection struct {
	endpoint string
	conn     net.Conn
	reader   *bufio.Reader
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

func NewClient(endpoints []string, size int) (*Client, error) {
	client := &Client{consistent: consistent.New(), connectionPools: make(map[string]chan *connection), endpoints: endpoints,
		poolSize: size}
	err := client.initPool()
	if err != nil {
		golog.Errorln("GlockClient - ", "Initing pool ", err)
		return nil, err
	}
	client.CheckServerStatus()

	golog.Debugf("Init with connection pool of %d to Glock server", size)
	return client, nil
}

func (c *Client) initPool() error {
	c.addEndpoints(c.endpoints)
	return nil
}

func (c *Client) addEndpoints(endpoints []string) {
	for _, endpoint := range endpoints {
		golog.Infoln("GlockClient -", "Attempting to add endpoint:", endpoint)
		conn, err := net.Dial("tcp", endpoint)
		if err == nil {
			c.connectionPools[endpoint] = make(chan *connection, c.poolSize)
			c.connectionPools[endpoint] <- &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: endpoint}
			c.consistent.Add(endpoint)
			golog.Infoln("GlockClient -", "Added endpoint:", endpoint)
		} else {
			golog.Errorln("GlockClient -", "Error adding endoint, could not connect, not added. endpoint:", endpoint, "error:", err)
		}
	}
}

func (c *Client) getConnection(key string) (*connection, error) {
	server, err := c.consistent.Get(key)
	if err != nil {
		golog.Errorln("GlockClient -", "Consistent hashing error, could not get server for key:", key, "error:", err)
		return nil, err
	}
	golog.Debugln("GlockClient -", "in getConn, got server", server, "for key", key)
	select {
	case conn := <-c.connectionPools[server]:
		return conn, nil
	default:
		golog.Infoln("GlockClient - Creating new connection... server:", server)
		conn, err := net.Dial("tcp", server)
		if err != nil {
			golog.Errorln("GlockClient - getConnection - could not connect to:", server, "error:", err)
			c.removeEndpoint(server)
			return nil, err
		}
		return &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: server}, nil
	}
}

func (c *Client) releaseConnection(connection *connection) {
	connectionPool, ok := c.connectionPools[connection.endpoint]

	if !ok {
		connection.Close()
		return
	}

	select {
	case connectionPool <- connection:
	default:
		connection.Close()
	}
}

func (c *Client) Lock(key string, duration time.Duration) (id int64, err error) {
	// its important that we get the server before we do getConnection (instead of inside getConnection) because if that error drops we need to put the connection back to the original mapping.

	connection, err := c.getConnection(key)
	if err != nil {
		return id, err
	}
	defer c.releaseConnection(connection)

	id, err = connection.lock(key, duration)
	if err != nil {
		if err, ok := err.(*glockError); ok {
			if err.errType == connectionErr {
				golog.Errorln("GlockClient -", "Connection error, couldn't get lock. Removing endpoint from hash table, server: ", connection.endpoint, " error: ", err)
				c.removeEndpoint(connection.endpoint)
				// todo for evan/treeder, if it is a connection error remove the failed server and then lock again recursively
				return c.Lock(key, duration)
			} else {
				golog.Errorln("GlockClient -", "unexpected error: ", err)
				return id, err
			}
		} else {
			golog.Errorln("GlockClient -", "Error trying to get lock. endpoint: ", connection.endpoint, " error: ", err)
			return id, err
		}
	}
	return id, nil
}

func (c *connection) lock(key string, duration time.Duration) (id int64, err error) {
	err = c.fprintf("LOCK %s %d\n", key, int(duration/time.Millisecond))
	if err != nil {
		golog.Errorln("GlockClient -", "lock error: ", err)
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

func (c *Client) removeEndpoint(endpoint string) {
	// remove from hash first
	c.consistent.Remove(endpoint)
	// then we should get rid of all the connections

	c.poolsLock.RLock()
	_, ok := c.connectionPools[endpoint]
	c.poolsLock.RUnlock()
	if !ok {
		return
	}

	c.poolsLock.Lock()
	defer c.poolsLock.Unlock()
	c.connectionPools[endpoint] = nil
}

func (c *Client) Unlock(key string, id int64) (err error) {

	connection, err := c.getConnection(key)
	if err != nil {
		return err
	}
	defer c.releaseConnection(connection)

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
