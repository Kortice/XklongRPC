package loadbalancer

import "github.com/Kotrice/XklongRPC/internal/registry"

type LoadBalancer interface {
	Select([]registry.Instance) registry.Instance
}
