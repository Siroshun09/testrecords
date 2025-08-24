package testrecords

import (
	"errors"
	"maps"
	"reflect"
	"testing"

	"github.com/huandu/go-sqlbuilder"
)

func Test_Inserter_flavor(t *testing.T) {
	tests := []struct {
		name     string
		inserter Inserter
		want     sqlbuilder.Flavor
	}{
		{
			name:     "NewInserter: MySQL",
			inserter: NewInserter(sqlbuilder.MySQL),
			want:     sqlbuilder.MySQL,
		},
		{
			name:     "NewInserter: PostgreSQL",
			inserter: NewInserter(sqlbuilder.PostgreSQL),
			want:     sqlbuilder.PostgreSQL,
		},
		{
			name:     "NewInserterForMySQL",
			inserter: NewInserterForMySQL(),
			want:     sqlbuilder.MySQL,
		},
		{
			name:     "NewInserterForPostgreSQL",
			inserter: NewInserterForPostgreSQL(),
			want:     sqlbuilder.PostgreSQL,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.inserter.flavor
			if tt.want != got {
				t.Errorf("flavor = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInserter_Add(t *testing.T) {
	tests := []struct {
		name      string
		inserter  Inserter
		tableName string
		records   []any
		want      Inserter
	}{
		{
			name:      "empty -> 1 table 1 record",
			inserter:  Inserter{},
			tableName: "test1",
			records:   []any{Record{id: 1}},
			want: Inserter{
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
		},
		{
			name:      "new inserter for MySQL -> 1 table 1 record",
			inserter:  NewInserterForMySQL(),
			tableName: "test1",
			records:   []any{Record{id: 1}},
			want: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
		},
		{
			name:      "new inserter for PostgreSQL -> 1 table 1 record",
			inserter:  NewInserterForPostgreSQL(),
			tableName: "test1",
			records:   []any{Record{id: 1}},
			want: Inserter{
				flavor: sqlbuilder.PostgreSQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
		},
		{
			name: "1 table 1 record -> different table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
			tableName: "test2",
			records:   []any{Record{id: 2}},
			want: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1", "test2"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
					"test2": {Record{id: 2}},
				},
			},
		},
		{
			name: "1 table 1 record -> same table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
			tableName: "test1",
			records:   []any{Record{id: 2}},
			want: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}, Record{id: 2}},
				},
			},
		},
		{
			name: "1 table 1 record -> add no records to same table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
			tableName: "test1",
			records:   []any{},
			want: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
		},
		{
			name: "1 table 1 record -> add no records to different table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
			tableName: "test2",
			records:   []any{},
			want: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.inserter
			if got := tt.inserter.Add(tt.tableName, tt.records...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Add() = %v, want %v", got, tt.want)
			}

			if !original.equals(tt.inserter) {
				t.Errorf("original = %v, want %v", original, tt.inserter)
			}
		})
	}
}

func TestInserter_InsertAll(t *testing.T) {
	tests := []struct {
		name              string
		inserter          Inserter
		expectedCallCount int
		wantErr           bool
	}{
		{
			name: "no table",
			inserter: Inserter{
				flavor:         sqlbuilder.MySQL,
				tables:         []string{},
				recordsByTable: map[string][]any{},
			},
			expectedCallCount: 0,
			wantErr:           false,
		},
		{
			name: "1 table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test"},
				recordsByTable: map[string][]any{
					"test": {Record{id: 1}},
				},
			},
			expectedCallCount: 1,
			wantErr:           false,
		},
		{
			name: "2 tables",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1", "test2"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
					"test2": {Record{id: 2}},
				},
			},
			expectedCallCount: 2,
			wantErr:           false,
		},
		{
			name: "error",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
			expectedCallCount: 1,
			wantErr:           true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mockedConn{}
			if tt.wantErr {
				conn.returnErr = errors.New("error")
			}

			if err := tt.inserter.InsertAll(t.Context(), conn); (err != nil) != tt.wantErr {
				t.Errorf("InsertAll() error = %v, wantErr %v", err, tt.wantErr)
			}

			if conn.callCount != tt.expectedCallCount {
				t.Errorf("conn.callCount = %v, want %v", conn.callCount, tt.expectedCallCount)
			}
		})
	}
}

func TestInserter_copy(t *testing.T) {
	capped := make([]any, 0, 10)
	capped = append(capped, Record{id: 1})

	existingRecords := []any{Record{id: 1}}

	tests := []struct {
		name     string
		inserter Inserter
		modifier func(i *Inserter)
	}{
		{
			name:     "empty -> 1 record added",
			inserter: Inserter{},
			modifier: func(i *Inserter) {
				i.tables = append(i.tables, "test1")
				i.recordsByTable["test1"] = []any{Record{id: 1}}
			},
		},
		{
			name:     "new inserter for MySQL -> 1 record added",
			inserter: NewInserterForMySQL(),
			modifier: func(i *Inserter) {
				i.tables = append(i.tables, "test1")
				i.recordsByTable["test1"] = []any{Record{id: 1}}
			},
		},
		{
			name:     "new inserter for PostgreSQL -> 1 record added",
			inserter: NewInserterForPostgreSQL(),
			modifier: func(i *Inserter) {
				i.tables = append(i.tables, "test1")
				i.recordsByTable["test1"] = []any{Record{id: 1}}
			},
		},
		{
			name: "1 record -> 1 record added for same table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": existingRecords,
				},
			},
			modifier: func(i *Inserter) {
				i.recordsByTable["test1"] = append(i.recordsByTable["test1"], Record{id: 2})
			},
		},
		{
			name: "1 record -> 1 record added for different table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": existingRecords,
				},
			},
			modifier: func(i *Inserter) {
				i.tables = append(i.tables, "test2")
				i.recordsByTable["test2"] = []any{Record{id: 2}}
			},
		},
		{
			name: "1 record with cap -> 1 record added for same table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": capped,
				},
			},
			modifier: func(i *Inserter) {
				i.recordsByTable["test1"] = append(i.recordsByTable["test1"], Record{id: 2})
			},
		},
		{
			name: "1 record -> edit by index",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": existingRecords,
				},
			},
			modifier: func(i *Inserter) {
				i.recordsByTable["test1"][0] = Record{id: 2}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copied := tt.inserter.copy()
			if !tt.inserter.equals(copied) {
				t.Errorf("copy() = %v, want %v", copied, tt.inserter)
			}

			tt.modifier(&copied)

			if tt.inserter.equals(copied) {
				t.Errorf("expected different Inserter after modification, but they are equal")
			}
		})
	}
}

func TestInserter_createQueryByTable(t *testing.T) {
	tests := []struct {
		name     string
		inserter Inserter
		count    int
	}{
		{
			name: "no table",
			inserter: Inserter{
				flavor:         sqlbuilder.MySQL,
				tables:         []string{},
				recordsByTable: map[string][]any{},
			},
			count: 0,
		},
		{
			name: "1 table",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
				},
			},
			count: 1,
		},
		{
			name: "2 tables",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1", "test2"},
				recordsByTable: map[string][]any{
					"test1": {Record{id: 1}},
					"test2": {Record{id: 2}},
				},
			},
			count: 2,
		},
		{
			name: "1 table, but no records",
			inserter: Inserter{
				flavor: sqlbuilder.MySQL,
				tables: []string{"test1"},
				recordsByTable: map[string][]any{
					"test1": {},
				},
			},
			count: 0,
		},
		{
			name: "tables contain 1 table, but not contained in recordsByTable",
			inserter: Inserter{
				flavor:         sqlbuilder.MySQL,
				tables:         []string{"test1"},
				recordsByTable: map[string][]any{},
			},
			count: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.inserter.createQueryByTable()
			count := len(maps.Collect(got))
			if count != tt.count {
				t.Errorf("createQueryByTable() = %v, want %v", count, tt.count)
			}
		})
	}
}
