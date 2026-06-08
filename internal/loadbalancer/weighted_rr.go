package loadbalancer

import (
	"log"
	"sync"

	"github.com/Kotrice/XklongRPC/internal/registry"
)

type WeightedRR struct {
	mu             sync.Mutex
	weights        []int // 固定权重
	currentWeights []int // 当前权重
	totalWeight    int   // 固定总权重
}

func NewWeightedRR(weights []int) *WeightedRR {
	w := &WeightedRR{
		weights:        make([]int, len(weights)),
		currentWeights: make([]int, len(weights)),
	}

	copy(w.weights, weights)

	for _, weight := range weights {
		w.totalWeight += weight
	}

	return w
}

func (w *WeightedRR) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// weights没更新
	if len(w.weights) != len(list) {
		log.Print("实例列表和权重列表大小不一致")
		return registry.Instance{}
	}

	maxIdx := -1
	for i := range w.currentWeights {
		w.currentWeights[i] += w.weights[i]

		if maxIdx < 0 || w.currentWeights[i] > w.currentWeights[maxIdx] {
			maxIdx = i
		}
	}

	w.currentWeights[maxIdx] -= w.totalWeight

	return list[maxIdx]
}
