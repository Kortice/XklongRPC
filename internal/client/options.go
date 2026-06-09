package client

import (
	"time"

	"github.com/Kotrice/XklongRPC/internal/codec"
	"github.com/Kotrice/XklongRPC/internal/loadbalancer"
)

type ClientOption func(*Client) error

func WithLoadBalancer(lb loadbalancer.LoadBalancer) ClientOption {
	return func(c *Client) error {
		c.lb = lb
		return nil
	}
}

func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		c.timeout = d
		return nil
	}
}

func WithCodec(t codec.Type) ClientOption {
	return func(c *Client) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		return nil
	}
}
