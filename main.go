package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/alecthomas/kong"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
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

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			query := msg.String
			log.Println("Got query", query)

			q, err := client.PrepareQuery(ctx, session.databaseName, query)
			if err != nil {
				return err
			}
			reader, err := q.Query(ctx)
			if err != nil {
				return err
			}
			fields := reader.Schema().Fields()
			log.Printf("fields %#v", fields)

			var fieldDescriptions []pgproto3.FieldDescription
			for _, f := range fields {
				fieldDescriptions = append(fieldDescriptions, pgproto3.FieldDescription{
					Name:                 []byte(f.Name),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				})
			}
			buf := (&pgproto3.RowDescription{Fields: fieldDescriptions}).Encode(nil)

			totalRows := 0
			for {
				batch, err := reader.Read()
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}

				nrows := int(batch.NumRows())
				totalRows += nrows

				bcols := batch.Columns()
				for r := 0; r < nrows; r++ {
					var cols [][]byte
					for c := range fields {
						b, err := render(bcols[c], r)
						if err != nil {
							return err
						}
						cols = append(cols, []byte(b))
					}
					buf = (&pgproto3.DataRow{Values: cols}).Encode(buf)
				}
			}

			buf = (&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", totalRows))}).Encode(buf)
			buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			reader.Release()
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

func render(column arrow.Array, row int) (string, error) {
	if column.IsNull(row) {
		return "NULL", nil
	}
	switch typedColumn := column.(type) {
	case *array.Timestamp:
		return typedColumn.Value(row).ToTime(arrow.Nanosecond).Format(time.RFC3339Nano), nil
	case *array.Float64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.String:
		return typedColumn.Value(row), nil
	case *array.Binary:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Boolean:
		return fmt.Sprint(typedColumn.Value(row)), nil
	default:
		return "", fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
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
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
	//	cli.OnExit()
}
