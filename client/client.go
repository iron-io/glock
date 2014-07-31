package glock

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stathat/consistent"
	"gopkg.in/inconshreveable/log15.v2"
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
	client.initPool()
	client.CheckServerStatus()

	log15.Debug("glock client init", "pool_size", size)
	return client, nil
}

func (c *Client) initPool() {
	c.addEndpoints(c.endpoints)
}

func (c *Client) addEndpoints(endpoints []string) {
	for _, endpoint := range endpoints {
		log15.Info("glock client adding endpoint", "endpoint", endpoint)
		conn, err := dial(endpoint, c.username, c.password)
		if err == nil {
			pool := make(chan *connection, c.poolSize)
			pool <- &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: endpoint, client: c}

			c.poolsLock.Lock()
			c.connectionPools[endpoint] = pool
			c.poolsLock.Unlock()

			c.countLock.Lock()
			c.connectionCount[endpoint] = new(int32)
			c.countLock.Unlock()

			c.consistent.Add(endpoint)
			log15.Info("glock client added endpoint", "endpoint", endpoint)
		} else {
			log15.Error("glock client error adding endpoint", "endpoint", endpoint, "err", err)
		}
	}
}

func (c *Client) getConnection(key string) (*connection, error) {
	server, err := c.consistent.Get(key)
	if err != nil {
		log15.Error("glock client consistent hashing error", "key", key, "err", err)
		return nil, err
	}
	log15.Debug("glock client in getConn", "server", server, "key", key)

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
		log15.Info("glock client creating new connection", "server", server)
		conn, err := dial(server, c.username, c.password)
		if err != nil {
			log15.Error("glock client getConnection could not connect", "server", server, "err", err)
			c.removeEndpoint(server)
			return nil, err
		}
		return &connection{conn: conn, reader: bufio.NewReader(conn), endpoint: server, client: c}, nil
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
			log15.Error("glock client connection error, couldn't get lock. Removing endpoint from hash table", "server", connection.endpoint, "err", err)
			c.removeEndpoint(connection.endpoint)
			// todo for evan/treeder, if it is a connection error remove the failed server and then lock again recursively
			return c.Lock(key, duration)
		}
		log15.Error("glock client error trying to get lock", "endpoint", connection.endpoint, "err", err)
		return id, err
	}
	return id, nil
}

func (c *connection) lock(key string, duration time.Duration) (id int64, err error) {
	err = c.fprintf("LOCK %s %d\r\n", key, int(duration/time.Millisecond))
	if err != nil {
		log15.Error("glock client lock error", "err", err)
		return id, err
	}

	splits, err := c.readResponse()
	if err != nil {
		log15.Error("glock client lock readResponse", "err", err)
		return id, err
	}

	id, err = strconv.ParseInt(splits[1], 10, 64)
	if err != nil {
		return id, &internalError{err}
	}

	return id, nil
}

func (c *Client) removeEndpoint(endpoint string) {
	log15.Error("glock client removing endpoint", "endpoint", endpoint)
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

	err = connection.fprintf("UNLOCK %s %d\r\n", key, id)
	if err != nil {
		log15.Error("glock client unlock error", "err ", err)
		return err
	}

	splits, err := connection.readResponse()
	if err != nil {
		log15.Error("glock client unlock readResponse error", "err", err)
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
				return &internalError{err}
			}
		} else {
			break
		}
	}
	return nil
}

func (c *connection) readResponse() (splits []string, err error) {
	splits, err = ReadSplits(c.reader)
	if err != nil {
		return nil, err
	}

	return splits, nil
}

func (c *connection) redial() error {
	c.conn.Close()
	conn, err := dial(c.endpoint, c.client.username, c.client.password)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)

	return nil
}

func dial(endpoint, username, password string) (net.Conn, error) {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}

	if username != "" {
		err = authenticateConn(conn, username, password)
		if err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (c *connection) Close() error {
	c.reader = nil
	return c.conn.Close()
}

func ReadSplits(reader *bufio.Reader) ([]string, error) {
	response, err := reader.ReadString('\n')
	log15.Debug("glock client glockResponse", "response", response)
	if err != nil {
		return nil, &connectionError{err}
	}

	trimmedResponse := strings.TrimRight(response, "\r\n")
	splits := strings.Split(trimmedResponse, " ")
	if splits[0] == "ERROR" {
		if splits[1] == "503" {
			return nil, &CapacityError{errors.New(trimmedResponse)}
		}
		return nil, &internalError{errors.New(trimmedResponse)}
	}

	return splits, nil
}

func authenticateConn(conn net.Conn, username, password string) error {
	// Step 1: Pass in username for challenge
	_, err := fmt.Fprintf(conn, "AUTH %s\r\n", username)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	splits, err := ReadSplits(reader)
	if err != nil {
		return err
	}

	authKeyBase64 := splits[0]
	authKey, err := base64.StdEncoding.DecodeString(authKeyBase64)
	if err != nil {
		return err
	}

	// Step 2: Pass in hashed authKey to get authenticated
	mac := hmac.New(sha256.New, authKey)
	mac.Write([]byte(password))
	expectedMAC := mac.Sum(nil)
	expectedMACBase64 := base64.StdEncoding.EncodeToString(expectedMAC)
	_, err = fmt.Fprintf(conn, "AUTH %s %s\r\n", username, expectedMACBase64)
	if err != nil {
		return err
	}

	splits, err = ReadSplits(reader)
	if err != nil {
		return err
	}
	if splits[0] != "AUTHORIZED" {
		return errors.New(strings.Join(splits, " "))
	}

	// Step 3: Successfully authenticated
	return nil
}
