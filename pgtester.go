package pgtester

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/jmoiron/sqlx"
)

// PGT helps to test Go PostgreSQL code.
type PGT struct {
	db           *sqlx.DB
	dbConnString string
	schema       Schema
}

// New returns a new instance of PGT.
func New(dbConnString string, schema Schema) (*PGT, error) {
	db, err := sqlx.Open("postgres", dbConnString)
	if err != nil {
		return nil, err
	}
	return &PGT{
		db:           db,
		dbConnString: dbConnString,
		schema:       schema,
	}, nil
}

// Schema describes the schema
type Schema map[string]TableSchema

// TableSchema describes the schema of an individual table from the database.
type TableSchema struct {
	SetupSQL string
	Deps     []string
}

// Runner runs a PostgreSQL test in isolation from other parallel tests.
func (p *PGT) Runner(t *testing.T, tables []string, testFn func(t *testing.T, sdb *sqlx.DB)) {
	schemaName := randomID()
	if _, err := p.db.Exec("CREATE SCHEMA " + schemaName); err != nil {
		t.Fatal(err)
	}
	defer p.db.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
	sdb, err := sqlx.Open("postgres", p.dbConnString+" search_path="+schemaName)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	for _, d := range p.resolveDeps(tables) {
		if _, err := sdb.Exec(p.schema[d].SetupSQL); err != nil {
			t.Fatal()
		}
	}
	testFn(t, sdb)
}

func (p *PGT) resolveDeps(ts []string) []string {
	var results []string
	for _, t := range ts {
		results = append(results, append(p.depsWalk(t), t)...)
	}
	exists := make(map[string]bool)
	var final []string
	for _, r := range results {
		if _, ok := exists[r]; !ok {
			final = append(final, r)
			exists[r] = true
		}
	}
	return final
}

func (p *PGT) depsWalk(t string) []string {
	var result []string
	for _, d := range p.schema[t].Deps {
		if len(p.schema[d].Deps) > 0 {
			result = append(result, p.depsWalk(d)...)
		}
		result = append(result, d)
	}
	return result
}

func randomID() string {
	var abc = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var buf bytes.Buffer
	for i := 0; i < 10; i++ {
		buf.WriteByte(abc[rand.Intn(len(abc))])
	}
	return buf.String()
}
