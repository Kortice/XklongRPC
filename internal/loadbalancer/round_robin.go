package loadbalancer

import (
	"sync/atomic"

	"github.com/Kotrice/XklongRPC/internal/registry"
)

type RoundRobin struct {
	idx uint64
}

func NewRR() *RoundRobin {
	return &RoundRobin{}
}

func (rr *RoundRobin) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}

	i := atomic.AddUint64(&rr.idx, 1)
	return list[i%uint64(len(list))]
}
