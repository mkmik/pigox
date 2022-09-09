package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	influxdbiox "github.com/influxdata/influxdb-iox-client-go"
	"github.com/jackc/pgproto3/v2"
)

type Context struct {
	*CLI
}

type CLI struct {
	ListenAddress string `optional:"" default:"localhost:1234"`
	IOxAddress    string `optional:"" default:"localhost:8082"`
}

type session struct {
	databaseName string
	userName     string
	token        string
}

type proxy struct {
	ioxAddress string
	backend    *pgproto3.Backend
	conn       net.Conn
	client     *influxdbiox.Client
}

func newProxy(conn net.Conn, ioxAddress string) proxy {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return proxy{
		ioxAddress: ioxAddress,
		backend:    backend,
		conn:       conn,
	}
}

func (p *proxy) testConnection(ctx context.Context, session *session) error {
	q, err := p.client.PrepareQuery(ctx, session.databaseName, "select 1")
	if err != nil {
		return err
	}
	reader, err := q.Query(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()
	return nil
}

func (p *proxy) Run() error {
	defer p.Close()

	session, err := p.handleStartup()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.client, err = influxdbiox.NewClient(ctx, &influxdbiox.ClientConfig{
		Address:  p.ioxAddress,
		Database: session.databaseName,
	})
	if err != nil {
		return err
	}

	if err := p.testConnection(ctx, session); err != nil {
		log.Printf("cannot connect downstream: %v", err)
		return writeError(p.conn, "FATAL", err)
	}

	if err := writeMessages(p.conn, &pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
		return fmt.Errorf("error writing query response: %w", err)
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

			if q := strings.TrimSpace(query); q == "" || q == ";" {
				log.Printf("Return empty query response")
				if err := writeMessages(p.conn, &pgproto3.EmptyQueryResponse{}); err != nil {
					return fmt.Errorf("error writing query response: %w", err)
				}
			} else {
				if _, err := p.processQuery(ctx, query, session); err != nil {
					log.Println(err)
				}
			}
			if err := writeMessages(p.conn, &pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
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

func (p *proxy) processQuery(ctx context.Context, query string, session *session) (totalRows int, err error) {
	defer func() {
		if err == nil {
			err = writeMessages(p.conn, &pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", totalRows))})
		} else {
			err = writeError(p.conn, "ERROR", err)
		}
	}()

	q, err := p.client.PrepareQuery(ctx, session.databaseName, query)
	if err != nil {
		return 0, err
	}
	reader, err := q.Query(ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Release()

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

	for {
		batch, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		nrows := int(batch.NumRows())
		totalRows += nrows

		bcols := batch.Columns()
		for r := 0; r < nrows; r++ {
			var cols [][]byte
			for c := range fields {
				b, err := render(bcols[c], r)
				if err != nil {
					return 0, err
				}
				cols = append(cols, []byte(b))
			}
			buf = (&pgproto3.DataRow{Values: cols}).Encode(buf)
		}
		_, err = p.conn.Write(buf)
		if err != nil {
			return 0, fmt.Errorf("error writing query response: %w", err)
		}
		buf = buf[:0] // reset slice without deallocating memory
	}

	return totalRows, nil
}

func (p *proxy) handleStartup() (*session, error) {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return nil, fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		err := writeMessages(p.conn, &pgproto3.AuthenticationCleartextPassword{})
		if err != nil {
			return nil, fmt.Errorf("error sending request for password: %w", err)
		}
		authMessage, err := p.backend.Receive()
		if err != nil {
			return nil, fmt.Errorf("error sending request for password: %w", err)
		}
		password, ok := authMessage.(*pgproto3.PasswordMessage)
		if !ok {
			return nil, fmt.Errorf("unexpected message %T", authMessage)
		}
		err = writeMessages(p.conn,
			&pgproto3.AuthenticationOk{},
			&pgproto3.ParameterStatus{Name: "server_version", Value: "14.2"},
			&pgproto3.ParameterStatus{Name: "client_encoding", Value: "utf8"},
			&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"},
		)
		if err != nil {
			return nil, fmt.Errorf("error sending ready for query: %w", err)
		}
		log.Printf("parameters %#v", startupMessage.Parameters)
		return &session{
			databaseName: startupMessage.Parameters["database"],
			userName:     startupMessage.Parameters["user"],
			token:        password.Password,
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
	case *array.Duration:
		m := typedColumn.DataType().(*arrow.DurationType).Unit.Multiplier()
		return (time.Duration(typedColumn.Value(row)) * m).String(), nil
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

// writeMessages writes all messages to a single buffer before sending.
func writeMessages(w io.Writer, msgs ...pgproto3.Message) error {
	var buf []byte
	for _, msg := range msgs {
		buf = msg.Encode(buf)
	}
	_, err := w.Write(buf)
	return err
}

func writeError(w io.Writer, severity string, err error) error {
	return writeMessages(w, &pgproto3.ErrorResponse{
		Severity:            severity,
		SeverityUnlocalized: severity,
		Code:                "XX000", // "internal_error"
		Message:             err.Error(),
	})
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

		b := newProxy(conn, cmd.IOxAddress)

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
