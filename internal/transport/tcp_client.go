package transport

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kotrice/XklongRPC/internal/protocol"
)

type TCPClient struct {
	conn *TCPConnection
	addr string

	writeMu sync.Mutex
	seq     uint64

	pending sync.Map // Map[uint64]*Future

	closed int32
}

// newTCPClient 返回 TCPClient 实例
func newTCPClient(addr string) (*TCPClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	tcpConn := NewTCPConnection(conn)

	c := &TCPClient{
		conn: tcpConn,
		addr: addr,
	}

	go c.readLoop()

	return c, nil
}

// nextSeq 获取下一个 seq
func (c *TCPClient) nextSeq() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}

// SendAsync 异步发送请求
func (c *TCPClient) SendAsync(msg *protocol.Message) (*Future, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("connectin closed")
	}

	seq := c.nextSeq()
	msg.Header.RequestID = seq

	future := NewFuture()
	c.pending.Store(seq, future)

	c.writeMu.Lock()
	err := c.conn.Write(msg)
	c.writeMu.Unlock()

	if err != nil {
		c.fail(err)
		c.pending.Delete(seq)
		return nil, err
	}

	return future, nil
}

// readLoop 循环等待数据回归
func (c *TCPClient) readLoop() {
	for {
		msg, err := c.conn.Read()
		if err != nil {
			c.fail(err)
			return
		}

		seq := msg.Header.RequestID
		val, ok := c.pending.LoadAndDelete(seq)
		if !ok {
			continue
		}

		future := val.(*Future)
		if msg.Header.Error != "" {
			future.Done(nil, errors.New(msg.Header.Error))
		} else {
			future.Done(msg.Body, nil)
		}
	}
}

// fail 异常处理
func (c *TCPClient) fail(err error) {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	// 关闭底层连接
	_ = c.conn.Close()

	// 处理 pending 中残留
	c.pending.Range(func(key, value any) bool {
		future := value.(*Future)
		future.Done(nil, err)
		c.pending.Delete(key)
		return true
	})
}

// Close 关闭客户端
func (c *TCPClient) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	return c.conn.Close()
}
