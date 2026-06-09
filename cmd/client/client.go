package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Kotrice/XklongRPC/internal/client"
	"github.com/Kotrice/XklongRPC/internal/codec"
	"github.com/Kotrice/XklongRPC/internal/registry"
	"github.com/Kotrice/XklongRPC/pkg/api"
)

func main() {
	reg, err := registry.NewRegistry([]string{"localhost:2379"})
	if err != nil {
		log.Fatal(err)
		return
	}

	cli, err := client.NewClient(reg, client.WithCodec(codec.JSON))
	if err != nil {
		log.Fatal(err)
		return
	}

	reply := &api.Reply{}

	if err = cli.Invoke(
		context.Background(),
		"Arith",
		"Add",
		api.Args{A: 3, B: 4},
		reply,
	); err != nil {
		log.Fatal(err)
		return
	}

	// except: 7
	fmt.Println(reply.Result)
}
