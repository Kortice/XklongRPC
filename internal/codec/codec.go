package codec

import (
	"fmt"
	"sync"
)

// 定义编码器统一接口
type Codec interface {
	Marshal(v any) ([]byte, error)      // 编码
	Unmarshal(data []byte, v any) error // 解码
}

// Type 用于标识不同的编码器类型
type Type byte

// Factory 定义编码器工厂函数
type Factory func() Codec

var (
	mu        sync.RWMutex
	factories = make(map[Type]Factory) // 初始化
)

// Register 注册编码器 t 以及对应工厂函数 f
func Register(t Type, f Factory) {
	mu.Lock()
	defer mu.Unlock()

	if f == nil {
		panic("codec: factory is nil")
	}

	if _, exists := factories[t]; exists {
		panic(fmt.Sprintf("codec: type %d already registered", t))
	}

	factories[t] = f
}

// New 根据 Type 返回 Codec 实例
func New(t Type) (Codec, error) {
	mu.RLock()
	defer mu.RUnlock()

	f, ok := factories[t]
	if !ok {
		return nil, fmt.Errorf("codec: type %d not registered", t)
	}

	return f(), nil
}
