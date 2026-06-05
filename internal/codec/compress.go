package codec

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"
)

// CompressionType 压缩类型
type CompressionType byte

const (
	CompressionNone = iota
	CompressionGzip
)

// compressor 压缩器接口
type compressor interface {
	compress(data []byte) ([]byte, error)
	decompress(data []byte) ([]byte, error)
}

// GzipCompressor 压缩器
type GzipCompressor struct{}

func (g *GzipCompressor) compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g *GzipCompressor) decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

var (
	gzipCompressor = &GzipCompressor{}
	compressorMu   sync.RWMutex
	compressors    = make(map[CompressionType]compressor)
)

// RegisterCompressor 注册压缩器
func RegisterCompressor(t CompressionType, c compressor) {
	compressorMu.Lock()
	defer compressorMu.Unlock()

	compressors[t] = c
}

// GetCompressor 获取压缩器
func GetCompressor(t CompressionType) compressor {
	compressorMu.RLock()
	defer compressorMu.RUnlock()

	return compressors[t]
}

func init() {
	RegisterCompressor(CompressionGzip, gzipCompressor)
}

// Compress 用指定类型压缩
func Compress(data []byte, t CompressionType) ([]byte, error) {
	c := GetCompressor(t)
	if c == nil {
		return nil, fmt.Errorf("codec: compressor not found")
	}
	return c.compress(data)
}

// Decompress 用指定类型解压
func Decompress(data []byte, t CompressionType) ([]byte, error) {
	c := GetCompressor(t)
	if c == nil {
		return nil, fmt.Errorf("codec: compressor not found")
	}
	return c.decompress(data)
}
