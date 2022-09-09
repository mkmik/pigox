package pigox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	influxdbiox "github.com/influxdata/influxdb-iox-client-go"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

var (
	sqlStringRe = regexp.MustCompile(`'((?:[^']|'')*)'`)
)

type session struct {
	databaseName string
	userName     string
	token        string
}

type pgError struct {
	error
	code string
}

func (p *pgError) Unwrap() error {
	return p.error
}

func newPGError(code string, err error) *pgError {
	return &pgError{
		error: err,
		code:  code,
	}
}

// Proxy is a PG->IOx proxy.
type Proxy struct {
	ioxAddress string
	backend    *pgproto3.Backend
	conn       net.Conn
	client     *influxdbiox.Client
}

// NewProxy creates a new PG->IOx proxy.
//
// ioxAddress is the address of the IOx gRPC API endpoint.
func NewProxy(conn net.Conn, ioxAddress string) Proxy {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return Proxy{
		ioxAddress: ioxAddress,
		backend:    backend,
		conn:       conn,
	}
}

func (p *Proxy) testConnection(ctx context.Context, session *session) error {
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

func (p *Proxy) runE() error {
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

	if session.token != "hunter12" {
		return newPGError(pgerrcode.InvalidPassword, fmt.Errorf("password authentication failed for user %q", session.userName))
	}

	if err := p.testConnection(ctx, session); err != nil {
		log.Printf("cannot connect downstream: %v", err)
		return err
	}

	err = writeMessages(p.conn,
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "14.2"},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "utf8"},
		&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"},
	)
	if err != nil {
		return fmt.Errorf("error sending ready for query: %w", err)
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
			log.Println("--------\nGot query", query)

			if q, err := rewriteQuery(query); err != nil {
				writeError(p.conn, "ERROR", err)
			} else {
				if q != query {
					log.Println("query rewritten")
				}
				query = q
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
			}
		case *pgproto3.Terminate:
			log.Println("got terminate message")
			return nil
		case *pgproto3.Parse:
			writeError(p.conn, "ERROR", newPGError(pgerrcode.FeatureNotSupported, fmt.Errorf("prepared statements are not yet implemented in IOx")))
		default:
			writeError(p.conn, "ERROR", newPGError(pgerrcode.FeatureNotSupported, fmt.Errorf("unsupported message type: %T", msg)))
		}

		// some clients expect a ReadForQuery message before reporiting the error message to the user.
		if err := writeMessages(p.conn, &pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
			return fmt.Errorf("error writing query response: %w", err)
		}
	}
}

func (p *Proxy) processQuery(ctx context.Context, query string, session *session) (totalRows int, err error) {
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

	var rowDesc pgproto3.RowDescription
	for _, f := range fields {
		rowDesc.Fields = append(rowDesc.Fields, pgproto3.FieldDescription{
			Name:                 []byte(f.Name),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          pgtype.TextOID,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	buf := rowDesc.Encode(nil)

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
			cols := make([][]byte, len(fields))
			for c := range fields {
				cols[c], err = renderBytes(bcols[c], r)
				if err != nil {
					return 0, err
				}
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

func (p *Proxy) handleStartup() (*session, error) {
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

func isInformational(query string) bool {
	return strings.Contains(query, "FROM pg_catalog.")
}

func rewriteQuery(query string) (string, error) {
	if isInformational(query) {
		if strings.Contains(query, `WHERE c.oid = i.inhrelid`) {
			return `select 1 limit 0;`, nil
		} else if strings.Contains(query, `WHERE c.oid = i.inhparent`) {
			return `select 1 limit 0;`, nil
		} else if strings.Contains(query, `WHERE p.puballtables AND`) {
			return `select 1 limit 0;`, nil
		} else if strings.Contains(query, `WHERE stxrelid =`) {
			return `select 1 limit 0`, nil
		} else if strings.Contains(query, `WHERE pol.polrelid =`) {
			return `select 1 limit 0`, nil
		} else if strings.Contains(query, `WHERE a.attrelid =`) {
			tableName := "air_temperature"
			return fmt.Sprintf(`select column_name as attname, data_type as format_type, '' as pg_get_expr, false as attnotnull, '' attcollation, '' as attidentity, '' as attgenerated from information_schema.columns where table_name='%s';`, tableName), nil
		} else if strings.Contains(query, `WHERE c.oid = `) {
			return `select 0 as relchecks, 'r' as relkind, false as relhasindex, false as relhasrules, false as relhastriggers, false as relrowsecurity, false as relforcerowsecurity, false as relhasoids, false as relispartition, '', 0 as reltablespace, '' as reloftype, 'p' as relpersistence, 'd' as relreplident, 'heap' as amname;`, nil
		} else if strings.Contains(query, `OPERATOR(pg_catalog.~)`) {
			groups := sqlStringRe.FindStringSubmatch(query)
			if len(groups) < 2 {
				return "", fmt.Errorf(`"\d <table>" is not supported`)
			}
			tableName := groups[1][2 : len(groups[1])-2]
			return fmt.Sprintf(`select '%s' as oid, 'iox' as nspname, '%s' as relname`, tableName, tableName), nil
		}
		if strings.Contains(query, `AND n.nspname <> 'pg_catalog'`) {
			return `select table_schema as "Schema", table_name as "Name", table_type as "Type", '' as "Owner" from information_schema.tables where table_schema not in ('system', 'information_schema');`, nil
		}
		return `select table_schema as "Schema", table_name as "Name", table_type as "Type", '' as "Owner" from information_schema.tables;`, nil
	}
	return query, nil
}

func renderText(column arrow.Array, row int) (string, error) {
	if column.IsNull(row) {
		return "NULL", nil
	}
	switch typedColumn := column.(type) {
	case *array.Timestamp:
		unit := typedColumn.DataType().(*arrow.TimestampType).Unit
		return typedColumn.Value(row).ToTime(unit).Format(time.RFC3339Nano), nil
	case *array.Time32:
		unit := typedColumn.DataType().(*arrow.Time32Type).Unit
		return typedColumn.Value(row).ToTime(unit).Format(time.RFC3339Nano), nil
	case *array.Time64:
		unit := typedColumn.DataType().(*arrow.Time64Type).Unit
		return typedColumn.Value(row).ToTime(unit).Format(time.RFC3339Nano), nil
	case *array.Date32:
		return typedColumn.Value(row).ToTime().Format(time.RFC3339Nano), nil
	case *array.Date64:
		return typedColumn.Value(row).ToTime().Format(time.RFC3339Nano), nil
	case *array.Duration:
		m := typedColumn.DataType().(*arrow.DurationType).Unit.Multiplier()
		return (time.Duration(typedColumn.Value(row)) * m).String(), nil
	case *array.Float16:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Float32:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Float64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint8:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint16:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint32:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int8:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int16:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int32:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.String:
		return typedColumn.Value(row), nil
	case *array.Binary:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Boolean:
		if typedColumn.Value(row) {
			return "t", nil
		} else {
			return "f", nil
		}
	default:
		return "", newPGError(pgerrcode.FeatureNotSupported, fmt.Errorf("unsupported arrow type %q", column.DataType().Name()))
	}
}

func renderBytes(column arrow.Array, row int) ([]byte, error) {
	s, err := renderText(column, row)
	return []byte(s), err
}

// Close terminates a pigox proxy connection.
func (p *Proxy) Close() error {
	return p.conn.Close()
}

// Run runs the PG->IOx proxy protocol.
func (p *Proxy) Run() {
	defer p.Close()

	if err := p.runE(); err != nil {
		log.Println("writing error to conn: ", err)
		if err := writeError(p.conn, "FATAL", err); err != nil {
			log.Printf("cannot return error to client: %v", err)
		}
	}
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
	code := pgerrcode.InternalError
	var perr *pgError
	if errors.As(err, &perr) {
		code = perr.code
	}
	return writeMessages(w, &pgproto3.ErrorResponse{
		Severity:            severity,
		SeverityUnlocalized: severity,
		Code:                code,
		Message:             err.Error(),
	})
}
