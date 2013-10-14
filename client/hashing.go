package glock

import (
	"github.com/stathat/consistent"
)

func initServesPool(endpoints []string) *consistent.Consistent {
	cons := consistent.New()
	for _, endpoint := range endpoints {
		// First check if endpoint is live
		cons.Add(endpoint)
	}
	return cons
}
