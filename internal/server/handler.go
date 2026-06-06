package server

import (
	"fmt"
	"log"
	"reflect"

	"github.com/Kotrice/XklongRPC/internal/codec"
	"github.com/Kotrice/XklongRPC/internal/protocol"
	"github.com/Kotrice/XklongRPC/internal/transport"
)

type Handler struct {
	codec codec.Codec
}

// NewHandler 返回 Handler 实例
func NewHandler(opts ...HandlerOption) (*Handler, error) {
	h := &Handler{}

	for _, opt := range opts {
		if err := opt(h); err != nil {
			return nil, err
		}
	}

	if h.codec == nil {
		return nil, fmt.Errorf("handler codec must not be nil")
	}

	return h, nil
}

// Process 处理 RPC 请求
func (h *Handler) Process(conn *transport.TCPConnection, msg *protocol.Message, server any) {
	result, err := h.invoke(
		server,
		msg.Header.ServiceName,
		msg.Header.MethodName,
		msg.Body,
	)

	if err != nil {
		h.writeError(conn, msg.Header.RequestID, err.Error())
		return
	}

	var body []byte
	if result != nil {
		body, err = h.codec.Marshal(result)
		if err != nil {
			log.Println("marshal error: ", err)
			h.writeError(conn, msg.Header.RequestID, err.Error())
			return
		}
	}

	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   msg.Header.RequestID,
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}

	conn.Write(resp)
}

// writeError write Error back to Client
func (h *Handler) writeError(conn *transport.TCPConnection, requestID uint64, errMsg string) {
	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   requestID,
			Error:       errMsg,
			Compression: codec.CompressionGzip,
		},
	}

	conn.Write(resp)
}

// invoke Call service.Method()
func (h *Handler) invoke(service any, serviceName, methodName string, body []byte) (any, error) {
	serviceValue := reflect.ValueOf(service)
	method := serviceValue.MethodByName(methodName)
	if !method.IsValid() {
		return nil, fmt.Errorf("method not found %s:%s", serviceName, methodName)
	}

	methodType := method.Type()
	numIn := method.Type().NumIn()
	numOut := method.Type().NumOut()

	args := make([]reflect.Value, 0, numIn)

	// func (req *Req, reply *Resp) error
	if numIn == 2 &&
		numOut == 1 &&
		methodType.In(0).Kind() == reflect.Pointer &&
		methodType.In(1).Kind() == reflect.Pointer &&
		methodType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {

		// 构造 req
		reqType := methodType.In(0)
		req := reflect.New(reqType.Elem())

		if len(body) > 0 {
			if err := h.codec.Unmarshal(body, req.Interface()); err != nil {
				return nil, err
			}
		}

		// 构造 reply
		replyType := methodType.In(1)
		reply := reflect.New(replyType.Elem())

		args = append(args, req)
		args = append(args, reply)

		results := method.Call(args)

		// 处理 errro
		if errVal := results[0].Interface(); errVal != nil {
			return nil, errVal.(error)
		}

		return reply.Elem().Interface(), nil
	}

	return nil, nil
}
