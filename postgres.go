package sqldump

import (
	"database/sql"
	"fmt"
	"strings"
	"text/template"
	"time"
)

const (
	// Show the names of all tables in database, one per row.
	PG_SHOW_TABLES = `select tablename from pg_catalog.pg_tables where schemaname!='pg_catalog' and schemaname!='information_schema';`

	// This function dumps the SQL for a specified table.
	PG_SHOW_TABLE_SQL = `CREATE OR REPLACE FUNCTION public.show_create_table(p_table_name character varying)
	RETURNS SETOF text AS
  $BODY$
  DECLARE
	  v_table_ddl   text;
	  column_record record;
	  table_rec record;
	  constraint_rec record;
	  firstrec boolean;
  BEGIN
	  FOR table_rec IN
		  SELECT c.relname FROM pg_catalog.pg_class c
			  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				  WHERE relkind = 'r'
				  AND relname~ ('^('||p_table_name||')$')
				  AND n.nspname <> 'pg_catalog'
				  AND n.nspname <> 'information_schema'
				  AND n.nspname !~ '^pg_toast'
				  AND pg_catalog.pg_table_is_visible(c.oid)
			ORDER BY c.relname
	  LOOP
  
		  FOR column_record IN 
			  SELECT 
				  b.nspname as schema_name,
				  b.relname as table_name,
				  a.attname as column_name,
				  pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
				  CASE WHEN 
					  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
					   FROM pg_catalog.pg_attrdef d
					   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
					  'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
									FROM pg_catalog.pg_attrdef d
									WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
				  ELSE
					  ''
				  END as column_default_value,
				  CASE WHEN a.attnotnull = true THEN 
					  'NOT NULL'
				  ELSE
					  'NULL'
				  END as column_not_null,
				  a.attnum as attnum,
				  e.max_attnum as max_attnum
			  FROM 
				  pg_catalog.pg_attribute a
				  INNER JOIN 
				   (SELECT c.oid,
					  n.nspname,
					  c.relname
					FROM pg_catalog.pg_class c
						 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
					WHERE c.relname = table_rec.relname
					  AND pg_catalog.pg_table_is_visible(c.oid)
					ORDER BY 2, 3) b
				  ON a.attrelid = b.oid
				  INNER JOIN 
				   (SELECT 
						a.attrelid,
						max(a.attnum) as max_attnum
					FROM pg_catalog.pg_attribute a
					WHERE a.attnum > 0 
					  AND NOT a.attisdropped
					GROUP BY a.attrelid) e
				  ON a.attrelid=e.attrelid
			  WHERE a.attnum > 0 
				AND NOT a.attisdropped
			  ORDER BY a.attnum
		  LOOP
			  IF column_record.attnum = 1 THEN
				  v_table_ddl:='CREATE TABLE '||column_record.schema_name||'.'||column_record.table_name||' (';
			  ELSE
				  v_table_ddl:=v_table_ddl||',';
			  END IF;
  
			  IF column_record.attnum <= column_record.max_attnum THEN
				  v_table_ddl:=v_table_ddl||chr(10)||
						   '    "'||column_record.column_name||'" '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
			  END IF;
		  END LOOP;
  
		  firstrec := TRUE;
		  FOR constraint_rec IN
			  SELECT conname, pg_get_constraintdef(c.oid) as constrainddef 
				  FROM pg_constraint c 
					  WHERE conrelid=(
						  SELECT attrelid FROM pg_attribute
						  WHERE attrelid = (
							  SELECT oid FROM pg_class WHERE relname = table_rec.relname
						  ) AND attname='tableoid'
					  )
		  LOOP
			  v_table_ddl:=v_table_ddl||','||chr(10);
			  v_table_ddl:=v_table_ddl||'CONSTRAINT '||constraint_rec.conname;
			  v_table_ddl:=v_table_ddl||chr(10)||'    '||constraint_rec.constrainddef;
			  firstrec := FALSE;
		  END LOOP;
		  v_table_ddl:=v_table_ddl||');';
		  RETURN NEXT v_table_ddl;
	  END LOOP;
  END;
  $BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;`

	// This removes the table dumper from the database.
	PG_DROP_SHOW_TABLE_SQL = `DROP FUNCTION show_create_table(p_table_name varchar);`

	pgtpl = `-- Go SQL Dump {{ .DumpVersion }}
	--
	-- ------------------------------------------------------
	-- Server version	{{ .ServerVersion }}

	{{range .Tables}}
	--
	-- Table structure for table {{ .Name }}
	--
	DROP TABLE IF EXISTS {{ .Name }};
	{{ .Sequences }}
	{{ .SQL }};
	--
	-- Dumping data for table {{ .Name }}
	--
	{{ if .Values }}
	INSERT INTO {{ .Name }} VALUES {{ .Values }};
	{{ end }}
	{{ end }}
	-- Dump completed on {{ .CompleteTime }}
`

	PG_GET_SEQ_LIST = `SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';`
	PG_GET_SEQ      = `select
	sequence_schema,
	data_type,
	start_value,
	minimum_value,
	maximum_value,
	increment
from information_schema.sequences where sequence_name=$1
`
)

// DumpPostgres to file.
func (d *Dumper) DumpPostgres(data dump, list ...string) error {
	// Install the procedure to generate SQL for tables.
	err := d.installProcedure()
	if err != nil {
		return err
	}

	for _, name := range list {
		t, err := d.createPostgresTable(name)
		if err != nil {
			return err
		}
		data.Tables = append(data.Tables, t)
	}

	// Set complete time
	data.CompleteTime = time.Now().String()

	// Write dump to file
	t, err := template.New("mysqldump").Parse(pgtpl)
	if err != nil {
		return err
	}

	t.Execute(data.file, data)

	err = d.dropProcedure()
	return err
}

func getStringRows(rows *sql.Rows) ([]string, error) {
	list := []string{}
	for rows.Next() {
		var s sql.NullString
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}

		list = append(list, s.String)
	}
	return list, rows.Err()
}

// getpostgrestables returns the table names from a PostgreSQL database.
func (d *Dumper) getPostgresTables() ([]string, error) {
	rows, err := d.db.Query(PG_SHOW_TABLES)
	if err != nil {
		return nil, err
	}

	return getStringRows(rows)
}

func (d *Dumper) getPostgresSequences() ([]string, error) {
	rows, err := d.db.Query(PG_GET_SEQ_LIST)
	if err != nil {
		return nil, err
	}

	return getStringRows(rows)
}

// installProcedure installs the SQL generator plpgsql.
func (d *Dumper) installProcedure() error {
	_, err := d.db.Exec(PG_SHOW_TABLE_SQL)
	return err
}

// dropProcedure removes the SQL generator plpgsql.
func (d *Dumper) dropProcedure() error {
	// _, err := d.db.Exec(PG_DROP_SHOW_TABLE_SQL)
	// return err
	return nil
}

func (d *Dumper) createPostgresTable(name string) (*table, error) {
	var err error
	t := &table{Name: name}

	sequences, err := d.getPostgresSequences()
	if err != nil {
		return nil, err
	}

	buf := strings.Builder{}
	for _, seq := range sequences {
		s, err := d.createPostgresSequenceSQL(seq)
		if err != nil {
			return nil, err
		}

		buf.WriteString(s)
	}
	t.Sequences = buf.String()

	if t.SQL, err = d.createPostgresTableSQL(name); err != nil {
		return nil, err
	}

	if t.Values, err = d.createTableValues(name); err != nil {
		return nil, err
	}

	return t, nil
}

func (d *Dumper) createPostgresSequenceSQL(name string) (string, error) {
	var schema, datatype sql.NullString
	var start, min, max, inc int64
	err := d.db.QueryRow(PG_GET_SEQ, name).Scan(
		&schema, &datatype, &start, &min, &max, &inc,
	)
	if err != nil {
		return "", err
	}

	s := fmt.Sprintf(
		"CREATE SEQUENCE %s.%s\n\tINCREMENT %d\n\tSTART %d\n\tMINVALUE %d\n\tMAXVALUE %d\n\tCACHE 1;\n\n",
		schema.String, name, inc, start, min, max,
	)
	return s, nil
}

func (d *Dumper) createPostgresTableSQL(name string) (string, error) {
	var s sql.NullString
	err := d.db.QueryRow("select show_create_table($1);", name).Scan(&s)
	return s.String, err
}
