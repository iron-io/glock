package glock

import (
	"log"
	"net"
	"time"

	"github.com/stathat/consistent"
)

func initServersPool(endpoints []string) *consistent.Consistent {
	cons := consistent.New()
	addEndpoints(cons, endpoints)
	return cons
}

func addEndpoints(cons *consistent.Consistent, endpoints []string) {
	for _, endpoint := range endpoints {
		conn, err := net.Dial("tcp", endpoint)
		if err == nil {
			log.Println("Adding Endpoint to glock servers: ", endpoint)
			cons.Add(endpoint)
			conn.Close()
		}
	}
}

func (c *Client) CheckServerStatus() {
	ticker := time.Tick(1 * time.Second)
	go func() {
		for _ = range ticker {
			down := downServers(c.endpoints, c.consistent.Members())
			if len(down) > 0 {
				addEndpoints(c.consistent, down)
			}

			for _, server := range c.consistent.Members() {
				log.Println("Glock Server - ", server, ": ", len(c.connectionPools[server]))
			}
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
