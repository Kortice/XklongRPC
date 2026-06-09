package api

type Args struct {
	A int
	B int
}

type Reply struct {
	Result int
}

type Arith struct{}

func (a *Arith) Add(args *Args, reply *Reply) error {
	reply.Result = args.A + args.B
	return nil
}

func (a *Arith) Mul(args *Args, reply *Reply) error {
	reply.Result = args.A * args.B
	return nil
}
