package testrecords

import (
	"context"
	"database/sql"
)

type Record struct {
	id int `db:"id"`
}

type mockedConn struct {
	callCount int
	returnErr error
}

func (c *mockedConn) ExecContext(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	c.callCount++ // we don't need to assert the query and args
	return nil, c.returnErr
}
