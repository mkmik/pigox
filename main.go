package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/alecthomas/kong"
	influxdbiox "github.com/influxdata/influxdb-iox-client-go"
	"github.com/jackc/pgproto3/v2"
)

type Context struct {
	*CLI
}

type CLI struct {
	ListenAddress string `optional:"" default:"localhost:1234"`
}

type session struct {
	databaseName string
	userName     string
}

type proxy struct {
	backend *pgproto3.Backend
	conn    net.Conn
}

func newProxy(conn net.Conn) proxy {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return proxy{
		backend: backend,
		conn:    conn,
	}
}

func (p *proxy) Run() error {
	defer p.Close()

	session, err := p.handleStartup()
	if err != nil {
		return err
	}
	log.Printf("session %#v", session)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := influxdbiox.NewClient(ctx, &influxdbiox.ClientConfig{
		Address:  "localhost:8082",
		Database: session.databaseName,
	})
	if err != nil {
		return err
	}
	_ = client

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			log.Println("Got query", msg.String)

			response := []byte("foo")

			buf := (&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("fortune"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			}}).Encode(nil)
			buf = (&pgproto3.DataRow{Values: [][]byte{response}}).Encode(buf)
			buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
			buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			_, err = p.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Terminate:
			log.Println("got terminate message")
			return nil
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

func (p *proxy) handleStartup() (*session, error) {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return nil, fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			return nil, fmt.Errorf("error sending ready for query: %w", err)
		}
		return &session{
			databaseName: startupMessage.Parameters["database"],
			userName:     startupMessage.Parameters["user"],
		}, nil
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			return nil, fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	default:
		return nil, fmt.Errorf("unknown startup message: %#v", startupMessage)
	}
}

func (p *proxy) Close() error {
	return p.conn.Close()
}

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

		b := newProxy(conn)

		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}

	return nil
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
	//	cli.OnExit()
}
