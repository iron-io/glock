package glock

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

type Client struct {
	Endpoint string
}

func NewClient(endpoint string) *Client {
	return &Client{endpoint}
}

func (c *Client) LockEx(key string, duration int) (id int) {
	conn, err := net.Dial("tcp", c.Endpoint)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Fprintf(conn, "LOCK %s %d \r\n", key, duration)
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Fatalln(err)
	}
	splits := strings.Split(strings.TrimRight(status, "\r\n"), " ")
	id, err = strconv.Atoi(splits[1])
	if err != nil {
		log.Fatalln(err)
	}
	return id
}

func (c *Client) UnLock(key string, id int) {
	conn, err := net.Dial("tcp", c.Endpoint)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Fprintf(conn, "UNLOCK %s %d \r\n", key, id)
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(status)
}
