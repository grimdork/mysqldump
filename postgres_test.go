package sqldump_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/grimdork/sqldump"
	_ "github.com/lib/pq"
)

const pgtest = "pg-test"

func TestPostgres(t *testing.T) {
	// Open connection to database
	db, err := sql.Open("postgres", "host=localhost port=5432 dbname=orb user=orb password=admin sslmode=disable")
	if err != nil {
		t.Errorf("Error opening databse: %s\n", err.Error())
		t.FailNow()
	}

	// Register database with mysqldump
	os.Remove(filepath.Join(os.TempDir(), pgtest))
	dumper, err := sqldump.NewDumper(db, os.TempDir(), pgtest)
	dumper.SetMaxRows(1)
	if err != nil {
		t.Errorf("Error registering databse: %s\n", err.Error())
		t.FailNow()
	}

	// Dump database to file
	t.Logf("Dumping to %s", dumper.Path())
	err = dumper.Dump()
	if err != nil {
		t.Errorf("Error dumping: %s\n", err.Error())
		return
	}

	// Close dumper and connected database
	dumper.Close()
}
