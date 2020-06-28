package mysqldump

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"text/template"
	"time"
)

type table struct {
	Name   string
	SQL    string
	Values string
}

type dump struct {
	DumpVersion   string
	ServerVersion string
	Tables        []*table
	CompleteTime  string
}

const version = "0.5.0"

// Dump a MySQL/MariaDB or PostgreSQL database or selection of tables from same based on the options supplied through the dumper.
func (d *Dumper) Dump(filters ...string) error {
	list := make(map[string]interface{})
	for _, x := range filters {
		list[x] = nil
	}

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
		DumpVersion: version,
		Tables:      make([]*table, 0),
	}

	// Get server version, thereby identifying type.
	if data.ServerVersion, err = getServerVersion(d.db); err != nil {
		return err
	}

	if strings.Contains(data.ServerVersion, "PostgreSQL") {
		return d.DumpPostgres(filters...)
	}

	// Get tables
	tables, err := getMySQLTables(d.db)
	if err != nil {
		return err
	}

	// Get sql for each desired table
	if len(list) > 0 {
		for _, name := range tables {
			_, ok := list[name]
			if !ok {
				continue
			}
			if t, err := createMySQLTable(d.db, name); err == nil {
				data.Tables = append(data.Tables, t)
			} else {
				return err
			}
		}
	} else {
		for _, name := range tables {
			if t, err := createMySQLTable(d.db, name); err == nil {
				data.Tables = append(data.Tables, t)
			} else {
				return err
			}
		}
	}

	// Set complete time
	data.CompleteTime = time.Now().String()

	// Write dump to file
	t, err := template.New("mysqldump").Parse(mytpl)
	if err != nil {
		return err
	}
	if err = t.Execute(f, data); err != nil {
		return err
	}

	return nil
}

func getServerVersion(db *sql.DB) (string, error) {
	var server_version sql.NullString
	if err := db.QueryRow("SELECT version()").Scan(&server_version); err != nil {
		return "", err
	}
	return server_version.String, nil
}
