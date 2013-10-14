package glock

import (
	"net"

	"github.com/stathat/consistent"
)

func initServersPool(endpoints []string) *consistent.Consistent {
	cons := consistent.New()
	for _, endpoint := range endpoints {
		conn, err := net.Dial("tcp", endpoint)
		if err == nil {
			cons.Add(endpoint)
			conn.Close()
		}
	}
	return cons
}
