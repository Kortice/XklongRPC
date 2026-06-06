package server

import "github.com/Kotrice/XklongRPC/internal/codec"

type HandlerOption func(h *Handler) error

func WithHandlerCodec(t codec.Type) HandlerOption {
	return func(h *Handler) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		h.codec = cc
		return nil
	}
}
