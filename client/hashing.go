package glock

import (
	"github.com/stathat/consistent"
)

func initServersPool(endpoints []string) *consistent.Consistent {
	cons := consistent.New()
	for _, endpoint := range endpoints {
		// TODO: First check if endpoint is live
		cons.Add(endpoint)
	}
	return cons
}
