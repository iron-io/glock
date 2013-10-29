package glock

import (
	"fmt"
	"time"

	"github.com/iron-io/golog"
)

func (c *Client) CheckServerStatus() {
	ticker := time.Tick(60 * time.Second)
	go func() {
		for _ = range ticker {
			down := downServers(c.endpoints, c.consistent.Members())
			if len(down) > 0 {
				c.addEndpoints(down)
			}

			serverStatus := "Glock Server Status: \n"
			for _, server := range c.consistent.Members() {
				serverStatus += fmt.Sprintln(server, ": ", "available - ", len(c.connectionPools[server]), "total - ", int(*c.connectionCount[server])+len(c.connectionPools[server]))
			}
			golog.Infoln(serverStatus, len(down), "down servers: ", down)
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
