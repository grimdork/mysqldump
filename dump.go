package sqldump

import (
	"database/sql"
	"errors"
	"os"
	"strings"
)

type table struct {
	Name   string
	SQL    string
	Values string
}

type dump struct {
	file          *os.File
	DumpVersion   string
	ServerVersion string
	Tables        []*table
	CompleteTime  string
}

const version = "0.5.0"

// Dump a MySQL/MariaDB or PostgreSQL database or selection of tables from same based on the options supplied through the dumper.
func (d *Dumper) Dump(filters ...string) error {
	// Check dump directory
	if e, _ := exists(d.path); e {
		return errors.New("Dump '" + d.path + "' already exists.")
	}

	// Create dump file
	f, err := os.Create(d.path)
	if err != nil {
		return err
	}

	defer f.Close()
	data := dump{
		file:        f,
		DumpVersion: version,
		Tables:      make([]*table, 0),
	}

	// Get server version, thereby identifying type.
	if data.ServerVersion, err = d.getServerVersion(); err != nil {
		return err
	}

	if strings.Contains(data.ServerVersion, "PostgreSQL") {
		return d.DumpPostgres(data, filters...)
	}

	return d.DumpMySQL(data, filters...)
}

func (d *Dumper) getServerVersion() (string, error) {
	var server_version sql.NullString
	if err := d.db.QueryRow("SELECT version()").Scan(&server_version); err != nil {
		return "", err
	}
	return server_version.String, nil
}
