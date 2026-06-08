package breaker

import (
	"log"
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open         // 断开
	HalfOpen
)

type CircuitBreaker struct {
	mu sync.Mutex

	state State

	// 统计数据
	failureCount int
	successCount int

	// 配置参数
	windowSize       int           // 统计窗口大小
	failureThreshold float64       // 失败率阈值
	openTimeout      time.Duration // 熔断持续时间

	// 状态控制
	lastStateChange time.Time
	halfOpenProbe   bool // 半开状态下是否已有探测请求
}

func NewCircuitBreaker(windowSize int, failureThreshold float64, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            Closed,
		windowSize:       windowSize,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
		lastStateChange:  time.Now(),
	}
}

// Allow 是否允许发送请求
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case Closed:
		return true

	case Open:
		// 计算熔断时间

		// 可以恢复为半开
		if time.Since(cb.lastStateChange) > cb.openTimeout {
			cb.state = HalfOpen
			cb.halfOpenProbe = false
			return true
		}
		// 还是断开
		return false

	case HalfOpen:
		if cb.halfOpenProbe {
			return false
		}
		cb.halfOpenProbe = true
		return true
	}

	return true
}

// RecordSuccess 记录成功
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case Closed:
		cb.successCount++

	case HalfOpen:
		cb.toClosed()

	case Open:
		log.Print("理论不可能触发")
	}
}

// RecordFailure 记录失败
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case Closed:
		cb.failureCount++

		total := cb.successCount + cb.failureCount
		if total < cb.windowSize {
			return
		}

		rate := float64(cb.failureCount) / float64(total)
		if rate >= cb.failureThreshold {
			cb.toOpen()
		}
		cb.resetCounts()

	case HalfOpen:
		cb.toOpen()

	case Open:
		// 不用管
	}
}

// toClosed 状态切换成 closed
func (cb *CircuitBreaker) toClosed() {
	cb.state = Closed
	cb.lastStateChange = time.Now()
	cb.halfOpenProbe = false
	cb.resetCounts()
}

// toOpen 状态切换成 open
func (cb *CircuitBreaker) toOpen() {
	cb.state = Open
	cb.lastStateChange = time.Now()
	cb.halfOpenProbe = false
	cb.resetCounts()
}

// resetCount 重置统计
func (cb *CircuitBreaker) resetCounts() {
	cb.failureCount = 0
	cb.successCount = 0
}

// State 返回当前状态
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return cb.state
}
