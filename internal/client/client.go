package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Kotrice/XklongRPC/internal/breaker"
	"github.com/Kotrice/XklongRPC/internal/codec"
	"github.com/Kotrice/XklongRPC/internal/limiter"
	"github.com/Kotrice/XklongRPC/internal/loadbalancer"
	"github.com/Kotrice/XklongRPC/internal/protocol"
	"github.com/Kotrice/XklongRPC/internal/registry"
	"github.com/Kotrice/XklongRPC/internal/transport"
)

type Client struct {
	reg     *registry.Registry
	lb      loadbalancer.LoadBalancer
	limiter *limiter.TokenBucket
	timeout time.Duration
	codec   codec.Codec
	breaker sync.Map // map[string]*CircuitBreaker

	pools sync.Map // map[string]*transport.ConnectinPool
}

func NewClient(reg *registry.Registry, opts ...ClientOption) (*Client, error) {
	cli := &Client{
		reg:     reg,
		lb:      loadbalancer.NewRR(),
		limiter: limiter.NewTokenBucket(10000),
		timeout: 5 * time.Second,
	}

	for _, opt := range opts {
		if err := opt(cli); err != nil {
			return nil, err
		}
	}

	return cli, nil
}

// InvokeAsync 异步发送请求
func (c *Client) InvokeAsync(ctx context.Context, service, method string, args any) (*transport.Future, error) {

	if !c.limiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded")
	}

	addr, err := c.getAddr(service)
	if err != nil {
		return nil, err
	}

	br, err := c.getBreaker(service, addr)
	if err != nil {
		return nil, err
	}

	if !br.Allow() {
		return nil, fmt.Errorf("circuit breaker opened!")
	}

	pool, err := c.getPool(addr)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire()
	if err != nil {
		return nil, err
	}

	body, err := c.codec.Marshal(args)
	if err != nil {
		return nil, err
	}

	msg := &protocol.Message{
		Header: &protocol.Header{
			ServiceName: service,
			MethodName:  method,
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}

	future, err := conn.SendAsync(msg)
	if err != nil {
		br.RecordFailure()
		return nil, err
	}

	future.OnComplete(func(err error) {
		if err != nil {
			br.RecordFailure()
		} else {
			br.RecordSuccess()
		}
	})

	return future, nil
}

// Invoke 同步 = 异步 + 等待
func (c *Client) Invoke(ctx context.Context, service, method string, args, reply any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	future, err := c.InvokeAsync(ctx, service, method, args)
	if err != nil {
		return err
	}

	return future.GetResultWithContext(reply, ctx)
}

func (c *Client) getAddr(service string) (string, error) {
	if c.reg == nil {
		return "", fmt.Errorf("registry not configure")
	}

	instances, err := c.reg.Discover(service)
	if err != nil {
		return "", err
	}

	if len(instances) == 0 {
		return "", fmt.Errorf("no instance invalid")
	}

	instance := c.lb.Select(instances)
	log.Print("选择的地址为：", instance.Addr)
	return instance.Addr, nil
}

func (c *Client) getBreaker(service, addr string) (*breaker.CircuitBreaker, error) {
	key := service + "|" + addr

	if val, ok := c.breaker.Load(key); ok {
		return val.(*breaker.CircuitBreaker), nil
	}

	br := breaker.NewCircuitBreaker(
		10,
		0.6,
		5*time.Second,
	)

	actual, _ := c.breaker.LoadOrStore(key, br)
	return actual.(*breaker.CircuitBreaker), nil
}

func (c *Client) getPool(addr string) (*transport.ConnectionPool, error) {
	if val, ok := c.pools.Load(addr); ok {
		return val.(*transport.ConnectionPool), nil
	}

	pool := transport.NewConnectinoPool(addr, 1)
	actual, _ := c.pools.LoadOrStore(addr, pool)
	return actual.(*transport.ConnectionPool), nil
}

func (c *Client) Close() {
	c.pools.Range(func(key, value any) bool {
		pool := value.(*transport.ConnectionPool)
		pool.Close()
		return true
	})
}
