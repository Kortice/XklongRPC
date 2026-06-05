package transport

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrPoolClosed = errors.New("connection pool closed")

type ConnectionPool struct {
	addr string

	maxActive int

	conns []*TCPClient
	mu    sync.Mutex

	closed bool
	next   int
}

func NewConnectinoPool(addr string, maxActive int) *ConnectionPool {
	return &ConnectionPool{
		addr:      addr,
		maxActive: maxActive,
		conns:     make([]*TCPClient, 0, maxActive),
	}
}

// Acquire 请求连接
func (cp *ConnectionPool) Acquire() (*TCPClient, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return nil, ErrPoolClosed
	}

	// 如果有空位直接创建新的
	if len(cp.conns) < cp.maxActive {
		conn, err := newTCPClient(cp.addr)
		if err != nil {
			return nil, err
		}
		cp.conns = append(cp.conns, conn)
		return conn, nil
	}

	// 轮询
	for i := range cp.conns {
		idx := (i + cp.next) % len(cp.conns)
		conn := cp.conns[idx]

		if atomic.LoadInt32(&conn.closed) == 0 {
			cp.next = (cp.next + 1) % len(cp.conns)
			return conn, nil
		}

		// 删除死亡的 conn
		cp.conns = append(cp.conns[:idx], cp.conns[idx+1:]...)
		if len(cp.conns) == 0 {
			break
		}
	}

	// 如果全部closed
	conn, err := newTCPClient(cp.addr)
	if err != nil {
		return nil, err
	}

	cp.conns = append(cp.conns, conn)

	return conn, nil
}

// Close 关闭连接池
func (cp *ConnectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return
	}

	cp.closed = true

	for _, conn := range cp.conns {
		conn.Close()
	}
}
