package sqldump

import (
	"database/sql"
	"errors"
	"os"
	"strings"
)

type table struct {
	Name      string
	Sequences string
	SQL       string
	Values    string
}

type dump struct {
	file          *os.File
	DumpVersion   string
	ServerVersion string
	Tables        []*table
	Table         *table
	CompleteTime  string
}

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

	list := filters
	// Get server version, thereby identifying type.
	if data.ServerVersion, err = d.getServerVersion(); err != nil {
		return err
	}

	if strings.Contains(data.ServerVersion, "PostgreSQL") {
		if len(list) == 0 {
			list, err = d.getPostgresTables()
			if err != nil {
				return err
			}
		}

		return d.DumpPostgres(data, list...)
	}

	if len(list) == 0 {
		list, err = d.getMySQLTables()
		if err != nil {
			return err
		}
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

func (d *Dumper) createTableValues(name string) (string, error) {
	// Get Data
	rows, err := d.db.Query("SELECT * FROM " + name)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if len(columns) == 0 {
		return "", errors.New("No columns in table " + name + ".")
	}

	// Read data
	data_text := make([]string, 0)
	for rows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i, _ := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = "'" + value.String + "'"
			} else {
				dataStrings[key] = "null"
			}
		}

		data_text = append(data_text, "("+strings.Join(dataStrings, ",")+")")
	}

	return strings.Join(data_text, ","), rows.Err()
}
