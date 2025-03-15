package testrecords

import (
	"context"
	"database/sql"
)

type Conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
