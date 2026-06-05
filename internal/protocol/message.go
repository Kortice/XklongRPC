package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/Kotrice/XklongRPC/internal/codec"
)

const Magic uint16 = 0x1234

type Message struct {
	Header *Header
	Body   []byte
}

func Encode(msg *Message) ([]byte, error) {
	if msg.Header == nil {
		return nil, fmt.Errorf("header is nil")
	}

	bodyBytes := msg.Body

	var err error
	if msg.Header.Compression != codec.CompressionNone {
		bodyBytes, err = codec.Compress(bodyBytes, msg.Header.Compression)
		if err != nil {
			return nil, err
		}
	}

	headerCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}

	headerBytes, err := headerCodec.Marshal(msg.Header)
	if err != nil {
		return nil, err
	}

	headerLen := uint32(len(headerBytes))
	bodyLen := uint32(len(bodyBytes))

	total := 2 + 4 + 4 + headerLen + bodyLen
	buf := make([]byte, total)

	binary.BigEndian.PutUint16(buf[0:2], Magic)
	binary.BigEndian.PutUint32(buf[2:6], headerLen)
	binary.BigEndian.PutUint32(buf[6:10], bodyLen)

	copy(buf[10:10+headerLen], headerBytes)
	copy(buf[10+headerLen:], bodyBytes)

	return buf, nil
}

func Decode(data []byte) (*Message, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("data too short")
	}

	// 检查 Magic
	if binary.BigEndian.Uint16(data[0:2]) != Magic {
		return nil, fmt.Errorf("invalid magic number")
	}

	headerLen := binary.BigEndian.Uint32(data[2:6])
	bodyLen := binary.BigEndian.Uint32(data[6:10])

	// 检查是否 粘包
	totalLen := 10 + int(headerLen) + int(bodyLen)
	if len(data) < totalLen {
		return nil, fmt.Errorf("incomplete packet")
	}

	// 解码 header
	headerBytes := data[10 : 10+headerLen]

	headerCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}

	var header Header
	err = headerCodec.Unmarshal(headerBytes, &header)
	if err != nil {
		return nil, err
	}

	// 解压 bodyBytes
	bodyBytes := data[10+headerLen : 10+headerLen+bodyLen]
	if header.Compression != codec.CompressionNone {
		bodyBytes, err = codec.Decompress(bodyBytes, header.Compression)
		if err != nil {
			return nil, err
		}
	}

	return &Message{
		Header: &header,
		Body:   bodyBytes,
	}, nil
}

// DecodeHeaderLen 从字节切片中解析出 HeaderLen
func DecodeHeaderLen(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// DecodeBodyLen 从字节切片中解析出 BodyLen
func DecodeBodyLen(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}
