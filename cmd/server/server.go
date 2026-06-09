package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Kotrice/XklongRPC/internal/registry"
	"github.com/Kotrice/XklongRPC/internal/server"
	"github.com/Kotrice/XklongRPC/pkg/api"
)

func main() {
	reg, err := registry.NewRegistry([]string{"localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}

	srv := server.NewServer(":9090")
	if srv == nil {
		log.Print("server NewServer failed")
		return
	}

	// 注册 Arith 服务
	srv.Register("Arith", &api.Arith{})
	// 注册服务到 etcd
	err = reg.Register("Arith", registry.Instance{
		Addr: "localhost:9090",
	}, 10)
	if err != nil {
		log.Fatal(err)
	}

	signCh := make(chan os.Signal, 1)
	signal.Notify(signCh, os.Interrupt)

	go func() {
		<-signCh
		log.Println("graceful shutdown...")
		srv.Shutdown()
	}()

	log.Println("server started at :9090")
	srv.Start()

}
