package glock

import (
	"fmt"
	"log"
	"time"

	"github.com/stathat/consistent"
)

func (c *Client) initServersPool(endpoints []string) {
	cons := consistent.New()
	c.addEndpoints(cons, endpoints)
	c.consistent = cons
}

func (c *Client) addEndpoints(cons *consistent.Consistent, endpoints []string) {
	for _, endpoint := range endpoints {
		conn, err := c.dial(endpoint)
		if err == nil {
			log.Println("Adding Endpoint to glock servers: ", endpoint)
			cons.Add(endpoint)
			conn.Close()
		}
	}
}

func (c *Client) CheckServerStatus() {
	ticker := time.Tick(60 * time.Second)
	go func() {
		for _ = range ticker {
			down := downServers(c.endpoints, c.consistent.Members())
			if len(down) > 0 {
				c.addEndpoints(c.consistent, down)
			}

			serverStatus := "Glock Server Status: \n"
			for _, server := range c.consistent.Members() {
				serverStatus += fmt.Sprintln(server, ": ", len(c.connectionPools[server]))
			}
			log.Println(serverStatus, len(down), "down servers: ", down)
		}
	}()
}

func downServers(endpoints, upServers []string) (downServers []string) {
	for _, endpoint := range endpoints {
		isUp := false
		for _, member := range upServers {
			if endpoint == member {
				isUp = true
			}
		}
		if !isUp {
			downServers = append(downServers, endpoint)
		}
	}
	return downServers
}
