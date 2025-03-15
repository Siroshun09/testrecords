package testrecords

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/Siroshun09/serrors"
	"github.com/huandu/go-sqlbuilder"
)

type Inserter struct {
	flavor         sqlbuilder.Flavor
	tables         []string
	recordsByTable map[string][]any
}

func NewInserter(flavor sqlbuilder.Flavor) Inserter {
	return Inserter{
		flavor:         flavor,
		tables:         []string{},
		recordsByTable: map[string][]any{},
	}
}

func NewInserterForMySQL() Inserter {
	return NewInserter(sqlbuilder.MySQL)
}

func NewInserterForPostgreSQL() Inserter {
	return NewInserter(sqlbuilder.PostgreSQL)
}

func (i Inserter) Add(tableName string, records ...any) Inserter {
	if len(records) == 0 {
		return i
	}

	ret := Inserter{
		flavor:         i.flavor,
		tables:         slices.Clone(i.tables),
		recordsByTable: maps.Clone(i.recordsByTable),
	}

	if existingRecords, exists := ret.recordsByTable[tableName]; exists {
		existingRecords = append(existingRecords, records...)
		ret.recordsByTable[tableName] = existingRecords
		return ret
	}

	ret.tables = append(ret.tables, tableName)
	ret.recordsByTable[tableName] = records
	return ret
}

func (i Inserter) InsertAll(ctx context.Context, conn Conn) error {
	for sql, args := range i.createQueryByTable() {
		_, err := conn.ExecContext(ctx, sql, args...)
		if err != nil {
			return serrors.WithStackTrace(err)
		}
	}
	return nil
}

func (i Inserter) createQueryByTable() iter.Seq2[string, []any] {
	return func(yield func(string, []any) bool) {
		for _, tableName := range i.tables {
			records, ok := i.recordsByTable[tableName]
			if !ok || len(records) == 0 {
				continue
			}

			sql, args := sqlbuilder.NewStruct(records[0]).For(i.flavor).InsertInto(tableName, records...).Build()
			if !yield(sql, args) {
				return
			}
		}
	}
}
