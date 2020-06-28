package mysqldump

import (
	"database/sql"
	"errors"
	"path"
	"time"
)

// Dumper represents a database.
type Dumper struct {
	db       *sql.DB
	basename string
	dir      string
	path     string
}

// Path returns the full path of the generated dump.
func (d *Dumper) Path() string {
	return d.path
}

/*
	Register a database and return a new dumper.

	db: Database that will be dumped (https://golang.org/pkg/database/sql/#DB).
	dir: Path to the directory where the dumps will be stored.
	basename: Stem to be used to name each dump file. Uses time.Time.Format (https://golang.org/pkg/time/#Time.Format). format.
*/
func Register(db *sql.DB, dir, basename string) (*Dumper, error) {
	if !isDir(dir) {
		return nil, errors.New("Invalid directory")
	}

	name := time.Now().Format(basename)
	return &Dumper{
		db:       db,
		basename: basename,
		dir:      dir,
		path:     path.Join(dir, name),
	}, nil
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
