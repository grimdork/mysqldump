package sqldump

import (
	"database/sql"
	"errors"
	"text/template"
	"time"
)

const mytpl = `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


{{range .Tables}}
--
-- Table structure for table {{ .Name }}
--

DROP TABLE IF EXISTS {{ .Name }};
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
{{ .SQL }};
/*!40101 SET character_set_client = @saved_cs_client */;
--
-- Dumping data for table {{ .Name }}
--

LOCK TABLES {{ .Name }} WRITE;
/*!40000 ALTER TABLE {{ .Name }} DISABLE KEYS */;
{{ if .Values }}
INSERT INTO {{ .Name }} VALUES {{ .Values }};
{{ end }}
/*!40000 ALTER TABLE {{ .Name }} ENABLE KEYS */;
UNLOCK TABLES;
{{ end }}
-- Dump completed on {{ .CompleteTime }}
`

// DumpMySQL to file.
func (d *Dumper) DumpMySQL(data dump, list ...string) error {
	// Get sql for each desired table
	for _, name := range list {
		if t, err := d.createMySQLTable(name); err == nil {
			data.Tables = append(data.Tables, t)
		} else {
			return err
		}
	}

	// Set complete time
	data.CompleteTime = time.Now().String()

	// Write dump to file
	t, err := template.New("mysqldump").Parse(mytpl)
	if err != nil {
		return err
	}

	return t.Execute(data.file, data)
}

func (d *Dumper) getMySQLTables() ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := d.db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}
	return tables, rows.Err()
}

func (d *Dumper) createMySQLTable(name string) (*table, error) {
	var err error
	t := &table{Name: name}

	if t.SQL, err = d.createMySQLTableSQL(name); err != nil {
		return nil, err
	}

	if t.Values, err = d.createTableValues(name); err != nil {
		return nil, err
	}

	return t, nil
}

func (d *Dumper) createMySQLTableSQL(name string) (string, error) {
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := d.db.QueryRow("SHOW CREATE TABLE "+name).Scan(&table_return, &table_sql)
	if err != nil {
		return "", err
	}
	if table_return.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return table_sql.String, nil
}
