package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
)

var lastTestStmt *testStmt

type testDriver struct{}

func (testDriver) Open(name string) (driver.Conn, error) {
	return &testConn{}, nil
}

type testConnector struct {
	pingErr    error
	prepareErr error
}

func (c *testConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &testConn{pingErr: c.pingErr, prepareErr: c.prepareErr}, nil
}

func (c *testConnector) Driver() driver.Driver {
	return testDriver{}
}

type testConn struct {
	pingErr    error
	prepareErr error
}

func (c *testConn) Prepare(query string) (driver.Stmt, error) {
	return newTestStmt(), nil
}

func (c *testConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	return newTestStmt(), nil
}

func (c *testConn) Close() error {
	return nil
}

func (c *testConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (c *testConn) Ping(ctx context.Context) error {
	return c.pingErr
}

type testStmt struct {
	closed bool
}

func newTestStmt() *testStmt {
	stmt := &testStmt{}
	lastTestStmt = stmt
	return stmt
}

func (s *testStmt) Close() error {
	s.closed = true
	return nil
}

func (s *testStmt) NumInput() int {
	return -1
}

func (s *testStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not supported")
}

func (s *testStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &testRows{}, nil
}

func (s *testStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return &testRows{}, nil
}

type testRows struct {
	closed bool
	sent   bool
}

func (r *testRows) Columns() []string {
	return []string{"value"}
}

func (r *testRows) Close() error {
	r.closed = true
	return nil
}

func (r *testRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	dest[0] = "ok"
	r.sent = true
	return nil
}

func newTestSQLDB(pingErr error) *sql.DB {
	return newTestSQLDBWithPrepareErr(pingErr, nil)
}

func newTestSQLDBWithPrepareErr(pingErr, prepareErr error) *sql.DB {
	return sql.OpenDB(&testConnector{pingErr: pingErr, prepareErr: prepareErr})
}
