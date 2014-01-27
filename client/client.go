package glock

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iron-io/golog"
	"github.com/stathat/consistent"
	"encoding/json"
	"io"
	"fmt"
)

type connectionError struct {
	error
}

type internalError struct {
	error
}

type CapacityError struct {
	error
}

type Client struct {
	endpoints       []string
	consistent      *consistent.Consistent
	poolsLock       sync.RWMutex
	connectionPools map[string]chan *connection
	poolSize        int
	username        string
	password        string

	// some refactoring required to embed this as a part of connectionPools
	connectionCount map[string]*int32
	countLock       sync.RWMutex
}

type connection struct {
	endpoint string
	conn     net.Conn
	reader   *bufio.Reader
	client   *Client
}

// todo: move to a common models package or something for both client and server
type Request struct {
	Command  string
	Username string // not sure if we need a username?  A global token might be fine
	Token    string // for authentication, set in config
	Key      string
	Timeout  int
	Id       int64
}

type Response struct {
	Code  int
	Msg   string
	Id    int64
	Error error
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

func NewClient(endpoints []string, size int, username, password string) (*Client, error) {
	client := &Client{consistent: consistent.New(), connectionPools: make(map[string]chan *connection), endpoints: endpoints,
		poolSize: size, connectionCount: make(map[string]*int32), username: username, password: password}
	err := client.initPool()
	if err != nil {
		golog.Errorln("GlockClient - ", "Error initing pool ", err)
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

		c2, err := c.newConnection(endpoint)
		if err != nil {
			golog.Errorln("Error making new connection!")
			continue
		}

		pool := make(chan *connection, c.poolSize)
		pool <- c2

		c.poolsLock.Lock()
		c.connectionPools[endpoint] = pool
		c.poolsLock.Unlock()

		c.countLock.Lock()
		c.connectionCount[endpoint] = new(int32)
		c.countLock.Unlock()

		c.consistent.Add(endpoint)
		golog.Infoln("GlockClient -", "Added endpoint:", endpoint)
	}
}

func (c *Client) newConnection(endpoint string) (*connection, error) {
	c2 := &connection{endpoint: endpoint, client: c}
	err := c2.dial()
	if err != nil {
		return nil, err
	}
	return c2, nil
}

func (c *Client) getConnection(key string) (*connection, error) {
	server, err := c.consistent.Get(key)
	if err != nil {
		golog.Errorln("GlockClient -", "Consistent hashing error, could not get server for key:", key, "error:", err)
		return nil, err
	}
	golog.Debugln("GlockClient -", "in getConn, got server", server, "for key", key)

	c.poolsLock.RLock()
	connectionPool, ok := c.connectionPools[server]
	c.poolsLock.RUnlock()
	if !ok {
		return nil, errors.New("connectionPool removed")
	}

	c.countLock.Lock()
	atomic.AddInt32(c.connectionCount[server], 1)
	c.countLock.Unlock()

	select {
	case conn := <-connectionPool:
		return conn, nil
	default:
		golog.Infoln("GlockClient - Creating new connection... server:", server)
		c2, err := c.newConnection(server)
		if err != nil {
			golog.Errorln("GlockClient - getConnection - could not connect to:", server, "error:", err)
			c.removeEndpoint(server)
			return nil, err
		}
		return c2, nil
	}
}

func (c *Client) releaseConnection(connection *connection) {
	c.poolsLock.RLock()
	connectionPool, ok := c.connectionPools[connection.endpoint]
	c.poolsLock.RUnlock()
	if !ok {
		connection.Close()
		return
	}

	select {
	case connectionPool <- connection:
	default:
		connection.Close()
	}

	c.countLock.Lock()
	atomic.AddInt32(c.connectionCount[connection.endpoint], -1)
	c.countLock.Unlock()
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
		if err, ok := err.(*connectionError); ok {
			golog.Errorln("GlockClient -", "Connection error, couldn't get lock. Removing endpoint from hash table, server: ", connection.endpoint, " error: ", err)
			c.removeEndpoint(connection.endpoint)
			// todo for evan/treeder, if it is a connection error remove the failed server and then lock again recursively
			return c.Lock(key, duration)
		}
		golog.Errorln("GlockClient -", "Error trying to get lock. endpoint: ", connection.endpoint, " error: ", err)
		return id, err
	}
	return id, nil
}

func (c *connection) lock(key string, duration time.Duration) (int64, error) {
	request := Request{Command: "lock", Key: key, Timeout: int(duration / time.Millisecond)}
	response, err := c.sendRequest(request)
	if err != nil {
		golog.Errorln("GlockClient -", "lock error: ", err)
		return 0, err
	}
	return response.Id, nil
}

func (c *Client) removeEndpoint(endpoint string) {
	golog.Errorln("GlockClient -", "Removing endpoint: ", endpoint)
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
	if _, ok := c.connectionPools[endpoint]; ok {
		delete(c.connectionPools, endpoint)
	}
	c.poolsLock.Unlock()

	c.countLock.Lock()
	if _, ok := c.connectionCount[endpoint]; ok {
		delete(c.connectionCount, endpoint)
	}
	c.countLock.Unlock()
}

func (c *Client) Unlock(key string, id int64) (err error) {

	connection, err := c.getConnection(key)
	if err != nil {
		return err
	}
	defer c.releaseConnection(connection)

	request := Request{Command: "unlock", Key: key, Id: id}
	response, err := connection.sendRequest(request)
	if err != nil {
		golog.Errorln("GlockClient - ", "unlock error: ", err)
		return err
	}

	switch response.Code {
	case 204:
		return errors.New("NOT_UNLOCKED")
	case 200:
		return nil
	}
	return errors.New("Unknown reponse format")
}

func (c *connection) sendRequest(request Request) (*Response, error) {
	b, err := json.Marshal(request)
	if err != nil {
		return nil, &internalError{err}
	}
	for i := 0; i < 3; i++ {
		_, err = c.conn.Write(b)
		if err != nil {
			err = c.redial()
			if err != nil {
				return nil, &internalError{err}
			}
		} else {
			break
		}
	}
	return c.readResponse()
}

func (c *connection) readResponse() (*Response, error) {
	dec := json.NewDecoder(bufio.NewReader(c.conn))
	response := Response{}
	if err := dec.Decode(&response); err == io.EOF {
		c.redial() // should we redial here?
	} else if err != nil {
		golog.Errorln(err)
		return nil, err
	}
	return &response, nil
}

func (c *connection) dial() error {
	conn, err := net.Dial("tcp", c.endpoint)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	golog.Infoln("GlockClient -", "DIALED Attempting to add endpoint:", c.endpoint)
	if c.client.username != "" {
		golog.Infoln("Authenticating conn", c.client.username)
		err = c.authenticateConn()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *connection) redial() error {
	c.conn.Close()
	return c.dial()
}

func (c *connection) Close() error {
	c.reader = nil
	return c.conn.Close()
}

func (c *connection) authenticateConn() error {
	// Step 1: Pass in username for challenge
	request := Request{Command: "auth", Username: c.client.username, Token: c.client.password}
	response, err := c.sendRequest(request)
	if err != nil {
		golog.Errorln("GlockClient -", "auth failed: ", err)
		return err
	}
	if response.Code >= 400 {
		return fmt.Errorf("Auth failed", response.Msg)
	}
	return nil
}
