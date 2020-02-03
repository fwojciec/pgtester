package pgtester

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	_ "github.com/lib/pq" // required
)

var testSchema = Schema{
	"authors": TableSchema{
		SetupSQL: `authorsSetup`,
		Deps:     []string{"users"},
	},
	"books": TableSchema{
		SetupSQL: `booksSetup`,
	},
	"book_authors": TableSchema{
		SetupSQL: `bookAuthorsSetup`,
		Deps:     []string{"books", "authors"},
	},
	"book_trans_languages": TableSchema{
		SetupSQL: `bookTransLanguagesSetup`,
		Deps:     []string{"books", "languages"},
	},
	"catalogs": TableSchema{
		SetupSQL: `catalogsSetup`,
		Deps:     []string{"clients"},
	},
	"categories": TableSchema{
		SetupSQL: `categoriesSetup`,
	},
	"clients": TableSchema{
		SetupSQL: `clientsSetup`,
		Deps:     []string{"users"},
	},
	"client_territories": TableSchema{
		SetupSQL: `clientTerritoriesSetup`,
		Deps:     []string{"clients", "territories"},
	},
	"languages": TableSchema{
		SetupSQL: `languagesSetup`,
	},
	"profiles": TableSchema{
		SetupSQL: `profilesSetup`,
		Deps:     []string{"users"},
	},
	"teams": TableSchema{
		SetupSQL: `teamsSetup`,
		Deps:     []string{"users"},
	},
	"team_members": TableSchema{
		SetupSQL: `teamMembersSetup`,
		Deps:     []string{"teams"},
	},
	"territories": TableSchema{
		SetupSQL: `territoriesSetup`,
	},
	"users": TableSchema{
		SetupSQL: `usersSetup`,
	},
}

var pgt *PGT

func init() {
	var err error
	pgt, err = New("dbname=test_db sslmode=disable", testSchema, rand.NewSource(1))
	if err != nil {
		panic(err)
	}
}

func TestResolveDeps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in  []string
		out []string
	}{
		{[]string{"authors"}, []string{"users", "authors"}},
		{[]string{"books"}, []string{"books"}},
		{[]string{"book_authors"}, []string{"books", "users", "authors", "book_authors"}},
		{[]string{"book_trans_languages"}, []string{"books", "languages", "book_trans_languages"}},
		{[]string{"clients"}, []string{"users", "clients"}},
		{[]string{"client_territories"}, []string{"users", "clients", "territories", "client_territories"}},
		{[]string{"client_territories", "clients"}, []string{"users", "clients", "territories", "client_territories"}},
		{[]string{"languages", "clients"}, []string{"languages", "users", "clients"}},
		{[]string{"languages"}, []string{"languages"}},
		{[]string{"territories"}, []string{"territories"}},
		{[]string{"users"}, []string{"users"}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(strings.Join(tnSliceToStringSlice(tc.in), ", "), func(t *testing.T) {
			t.Parallel()
			res := pgt.resolveDeps(tc.in)
			equals(t, tc.out, res)
		})
	}
}

func tnSliceToStringSlice(in []string) []string {
	out := make([]string, len(in))
	for i, t := range in {
		out[i] = string(t)
	}
	return out
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
