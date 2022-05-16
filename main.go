package main

import (
	"log"
	"net"

	"github.com/alecthomas/kong"
	"github.com/mkmik/piggo/pigox"
)

// Context is a CLI context.
type Context struct {
	*CLI
}

// CLI contains the CLI parameters.
type CLI struct {
	ListenAddress string `optional:"" default:"localhost:1234"`
	IOxAddress    string `optional:"" default:"localhost:8082"`
}

// Run is the main body of the CLI.
func (cmd *CLI) Run(cli *Context) error {
	ln, err := net.Listen("tcp", cmd.ListenAddress)
	if err != nil {
		return err
	}
	log.Println("Listening on", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		b := pigox.NewProxy(conn, cmd.IOxAddress)
		go func() {
			b.Run()
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
	//	cli.OnExit()
}
