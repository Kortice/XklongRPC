# XklongRPC

从零实现的轻量级 Go RPC 框架，用于学习分布式系统核心组件。

## 架构

```
┌─────────────────────────────────────────────────────┐
│                      Client                          │
│  limiter → discovery → loadbalance → breaker → pool │
└──────────────────┬──────────────────────────────────┘
                   │  TCP (自定义二进制协议)
┌──────────────────▼──────────────────────────────────┐
│                      Server                          │
│         limiter → handler (reflection)               │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│               etcd (服务注册/发现)                     │
└─────────────────────────────────────────────────────┘
```

## 功能

- **自定义通信协议** — 二进制包格式：Magic(2B) + HeaderLen(4B) + BodyLen(4B) + Header(JSON) + Body(可压缩)
- **可插拔编解码** — JSON / Protobuf，工厂+注册模式，扩展新编码无需改现有代码
- **gzip 压缩** — 独立压缩器注册表，按类型动态压缩/解压
- **服务注册发现** — etcd Lease + KeepAlive 自动续约，Watch 实时同步变更
- **负载均衡** — 随机 / 轮询 / 平滑加权轮询
- **熔断器** — 三态模型（Closed → Open → HalfOpen），滑动窗口统计失败率
- **限流** — 令牌桶，每秒重置
- **连接池** — 轮询分发，自动剔除死连接
- **异步 Future 模式** — TCP 连接复用，RequestID 匹配乱序响应
- **反射调用** — 服务端 net/rpc 风格自动路由

## 项目结构

```
XklongRPC/
├── cmd/
│   ├── server/        # 服务端示例
│   └── client/        # 客户端示例
├── pkg/api/           # 示例服务定义
└── internal/
    ├── codec/         # 编解码 + 压缩
    ├── protocol/      # 通信协议
    ├── transport/     # TCP 传输 + Future + 连接池
    ├── server/        # 服务端 + 反射 handler
    ├── client/        # 客户端（集成限流/熔断/负载均衡/服务发现）
    ├── registry/      # etcd 服务注册发现
    ├── breaker/       # 熔断器
    ├── limiter/       # 限流器
    └── loadbalancer/  # 负载均衡
```

## 快速开始

**1. 启动 etcd**

```bash
etcd
```

**2. 启动服务端**

```bash
go run cmd/server/server.go
```

**3. 运行客户端**

```bash
go run cmd/client/client.go
# 输出: 7
```

## 请求链路

```
Client.Invoke()
  │
  ├── TokenBucket.Allow()        // 限流
  ├── Registry.Discover()        // etcd 服务发现
  ├── LoadBalancer.Select()      // 负载均衡
  ├── CircuitBreaker.Allow()     // 熔断判断
  ├── ConnectionPool.Acquire()   // 连接池
  ├── Codec.Marshal(args)        // 编码
  ├── Encode(Message)            // 组包
  ├── TCP Write                  // 发送
  │
  └── Future.Wait()              // 等待响应
        │
        ▼
Server.Handle()
  ├── Decode(bytes)              // 解包
  ├── TokenBucket.Allow()        // 限流
  ├── Handler.Process()          // 反射调用
  │   ├── MethodByName(method)
  │   ├── Codec.Unmarshal(body, req)
  │   └── method.Call(req, reply)
  ├── Codec.Marshal(reply)       // 编码结果
  └── TCP Write                  // 返回
```

## 关键技术点

- 二进制协议包格式：`[2B Magic][4B headerLen][4B bodyLen][header JSON][body]`
- header 固定 JSON 序列化（解决自举问题），body 支持 JSON/Protobuf
- RequestID 实现连接复用，`sync.Map` 存储 pending futures
- 熔断器：Closed（正常）→ Open（60% 失败率 / 10 次窗口 / 5 秒超时）→ HalfOpen（单探测恢复）
- 平滑加权轮询：`currentWeight += weight → 选 max → max -= totalWeight`

## License

AGPL-3.0
