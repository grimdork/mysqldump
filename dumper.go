package sqldump

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"
)

// Dumper represents a database.
type Dumper struct {
	db   *sql.DB
	path string
	step int64
	pg   bool
}

func NewDumper(db *sql.DB, dir, basename string) (*Dumper, error) {
	if !isDir(dir) {
		return nil, errors.New("Invalid directory")
	}

	path := filepath.Join(dir, time.Now().Format(basename))
	return &Dumper{
		db:   db,
		path: path,
		step: 1000,
	}, nil
}

// SetMaxRows sets the number of rows to fetch at a time.
// Default is 1000. Lower this if running out of memory or timing out.
func (d *Dumper) SetMaxRows(n int64) {
	if d.step > 0 {
		d.step = n
	}
}

// Closes the dumper.
// Will also close the database the dumper is connected to.
//
// Not required.
func (d *Dumper) Close() error {
	defer func() {
		d.db = nil
	}()
	return d.db.Close()
}

// Path returns the full path of the generated dump.
func (d *Dumper) Path() string {
	return d.path
}
