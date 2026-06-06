package server

import (
	"log"
	"net"

	"github.com/Kotrice/XklongRPC/internal/codec"
	"github.com/Kotrice/XklongRPC/internal/limiter"
	"github.com/Kotrice/XklongRPC/internal/protocol"
	"github.com/Kotrice/XklongRPC/internal/transport"
)

type Server struct {
	addr     string
	listener net.Listener
	handler  *Handler
	limiter  *limiter.TokenBucket
	services map[string]any

	conns   map[*transport.TCPConnection]struct{}
	closing chan struct{}
}

func mustNewHandler() *Handler {
	h, err := NewHandler(WithHandlerCodec(codec.JSON))
	if err != nil {
		panic(err)
	}
	return h
}

func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		handler:  mustNewHandler(),
		limiter:  limiter.NewTokenBucket(10000),
		services: make(map[string]any),
		conns:    make(map[*transport.TCPConnection]struct{}),
		closing:  make(chan struct{}),
	}
}

// Register 注册服务
func (s *Server) Register(name string, service any) {
	s.services[name] = service
}

// Start 启动 Server
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closing:
				return nil
			default:
				continue
			}
		}

		tcpConn := transport.NewTCPConnection(conn)
		s.conns[tcpConn] = struct{}{}

		go func() {
			s.Handle(tcpConn)
			delete(s.conns, tcpConn)
		}()
	}

}

// Handle 处理请求
func (s *Server) Handle(conn *transport.TCPConnection) {
	defer conn.Close()
	for {
		msg, err := conn.Read()
		if err != nil {
			return
		}

		// 限流检查
		if !s.limiter.Allow() {
			resp := &protocol.Message{
				Header: &protocol.Header{
					RequestID:   msg.Header.RequestID,
					Error:       "rate limit exceeded",
					Compression: codec.CompressionGzip,
				},
			}
			conn.Write(resp)
			continue
		}
		// 处理请求
		s.handler.Process(conn, msg, s.services[msg.Header.ServiceName])
	}
}

// Close 停止接受请求，仍在处理请求
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// Shutdown 关闭服务（停止接受+处理请求）
func (s *Server) Shutdown() {
	close(s.closing)

	if s.listener != nil {
		s.listener.Close()
	}

	for conn := range s.conns {
		conn.Close()
	}

	log.Println("server shutdown complete")
}
