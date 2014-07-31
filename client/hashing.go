package glock

import (
	"sync/atomic"
	"time"

	"gopkg.in/inconshreveable/log15.v2"
)

func (c *Client) CheckServerStatus() {
	go func() {
		ticker := time.Tick(60 * time.Second)
		for _ = range ticker {
			members := c.consistent.Members()
			down := downServers(c.endpoints, members)
			if len(down) > 0 {
				c.addEndpoints(down)
			}

			serverStatuses := make([]interface{}, 4*len(members))
			for i, server := range members {
				c.poolsLock.RLock()
				availableConns := len(c.connectionPools[server])
				c.poolsLock.RUnlock()
				totalConns := int(atomic.LoadInt32(c.connectionCount[server])) + availableConns
				j := 4 * i
				serverStatuses[j+0] = server + "_available"
				serverStatuses[j+1] = availableConns
				serverStatuses[j+2] = server + "_total"
				serverStatuses[j+3] = totalConns
			}
			log15.Info("glock server statuses", serverStatuses...)
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
