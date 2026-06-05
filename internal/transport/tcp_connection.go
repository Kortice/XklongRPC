package transport

import (
	"bufio"
	"net"
	"sync"

	"github.com/Kotrice/XklongRPC/internal/protocol"
)

const BufferSize = 4096 // 4KB

// 包缓冲区（处理粘包）
type PacketBuffer struct {
	buf  []byte
	lock sync.Mutex
}

// Write 向缓冲区写数据
func (pb *PacketBuffer) Write(data []byte) {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	pb.buf = append(pb.buf, data...)
}

// Read 从缓冲区读数据
func (pb *PacketBuffer) Read() []byte {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	// 检查 len
	if len(pb.buf) < 10 {
		return nil
	}

	headerLen := protocol.DecodeHeaderLen(pb.buf[2:6]) // 4B
	bodyLen := protocol.DecodeBodyLen(pb.buf[6:10])    // 4B

	total := 2 + 4 + 4 + headerLen + bodyLen
	if len(pb.buf) < int(total) {
		return nil
	}

	// 当前完整的包
	packet := make([]byte, total)
	copy(packet, pb.buf[:total])
	// 滑动窗口
	pb.buf = pb.buf[total:]

	return packet
}

type TCPConnection struct {
	conn   net.Conn
	reader *bufio.Reader
	buf    *PacketBuffer

	writeMu sync.Mutex
}

// NewTCPConnection 返回 TCPConnection 实例
func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, BufferSize),
		buf:    &PacketBuffer{buf: make([]byte, 0, 2*BufferSize)},
	}
}

// Read 读取数据
func (tc *TCPConnection) Read() (*protocol.Message, error) {
	for {
		// 尝试从缓冲区读取数据
		if packet := tc.buf.Read(); packet != nil {
			return protocol.Decode(packet)
		}

		tmp := make([]byte, BufferSize)
		n, err := tc.reader.Read(tmp)
		if err != nil {
			return nil, err
		}

		if n > 0 {
			tc.buf.Write(tmp[:n])
		}
	}
}

// Write 写数据
func (tc *TCPConnection) Write(msg *protocol.Message) error {
	data, err := protocol.Encode(msg)
	if err != nil {
		return err
	}

	tc.writeMu.Lock()
	defer tc.writeMu.Unlock()

	total := 0
	for total < len(data) {
		n, err := tc.conn.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}

	return nil
}

// Close 关闭连接
func (tc *TCPConnection) Close() error {
	if tcp, ok := tc.conn.(*net.TCPConn); ok {
		tcp.SetLinger(0)
	}
	return tc.conn.Close()
}

func (tc *TCPConnection) RemoteAddr() string {
	return tc.conn.RemoteAddr().String()
}
