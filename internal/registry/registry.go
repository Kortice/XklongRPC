package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Instance struct {
	Addr string
}

type Registry struct {
	client *clientv3.Client
	prefix string

	mu       sync.RWMutex
	services map[string]map[string]Instance // service -> addr -> instance

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRegistry 返回 Registry 实例
func NewRegistry(endpoints []string) (*Registry, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Registry{
		client:   client,
		prefix:   "/XklongRPC/services/",
		services: make(map[string]map[string]Instance),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Register 注册服务
func (r *Registry) Register(service string, ins Instance, ttl int64) error {
	// 获取 lease
	lease, err := r.client.Grant(r.ctx, ttl)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s%s/%s", r.prefix, service, ins.Addr)

	// Put key-value with lease
	_, err = r.client.Put(r.ctx, key, ins.Addr, clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}

	// keep lease alive
	ch, err := r.client.KeepAlive(r.ctx, lease.ID)
	if err != nil {
		return err
	}

	go func() {
		for {
			if _, ok := <-ch; !ok {
				return
			}
		}
	}()

	return nil
}

// Discover 服务发现
func (r *Registry) Discover(service string) ([]Instance, error) {
	r.mu.RLock()
	if _, ok := r.services[service]; ok {
		r.mu.RUnlock()
		return r.copyInstances(service), nil
	}
	r.mu.RUnlock()

	// initService
	if err := r.initService(service); err != nil {
		return nil, err
	}

	return r.copyInstances(service), nil
}

// initService 初始化服务
func (r *Registry) initService(service string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.services[service]; ok {
		return nil
	}

	key := fmt.Sprintf("%s%s/", r.prefix, service)

	// Get
	resp, err := r.client.Get(r.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	r.services[service] = make(map[string]Instance)

	for _, Kv := range resp.Kvs {
		addr := string(Kv.Value)
		r.services[service][addr] = Instance{Addr: addr}
	}

	go r.watch(service)

	return nil
}

// watch
func (r *Registry) watch(service string) {
	key := fmt.Sprintf("%s%s/", r.prefix, service)

	for {
		watchCh := r.client.Watch(r.ctx, key, clientv3.WithPrefix())
		for watchResp := range watchCh {
			for _, event := range watchResp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					addr := string(event.Kv.Value)
					r.mu.Lock()
					r.services[service][addr] = Instance{Addr: addr}
					r.mu.Unlock()
				case clientv3.EventTypeDelete:
					deleteKey := string(event.Kv.Key)
					addr := strings.TrimPrefix(deleteKey, r.prefix+service+"/")
					r.mu.Lock()
					delete(r.services[service], addr)
					r.mu.Unlock()
				}
			}
		}

		time.Sleep(time.Second)
	}
}

// copyInstances 复制Instances返回
func (r *Registry) copyInstances(service string) []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var instances []Instance

	for _, ins := range r.services[service] {
		instances = append(instances, ins)
	}

	return instances
}

// Close 关闭服务
func (r *Registry) Close() error {
	r.cancel()
	return r.client.Close()
}
