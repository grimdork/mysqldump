package sqldump

import (
	"database/sql"
	"fmt"
)

const (
	// Show the names of all tables in database, one per row.
	PG_SHOW_TABLES = `select tablename from pg_catalog.pg_tables where schemaname!='pg_catalog' and schemaname!='information_schema';`

	// This function dumps the SQL for a specified table.
	PG_SHOW_TABLE_SQL = `CREATE OR REPLACE FUNCTION show_table_sql(p_table_name varchar)
	RETURNS text AS
  $BODY$
  DECLARE
	  v_table_ddl   text;
	  column_record record;
  BEGIN
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
				WHERE c.relname ~ ('^('||p_table_name||')$')
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
					   '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
		  END IF;
	  END LOOP;
  
	  v_table_ddl:=v_table_ddl||');';
	  RETURN v_table_ddl;
  END;
  $BODY$
	LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER;`

	// This removes the table dumper from the database.
	PG_DROP_SHOW_TABLE_SQL = `DROP FUNCTION show_table_sql(p_table_name varchar);`

	pgtpl = `-- Go SQL Dump {{ .DumpVersion }}
	--
	-- ------------------------------------------------------
	-- Server version	{{ .ServerVersion }}

	{{range .Tables}}
	--
	-- Table structure for table {{ .Name }}
	--
	
	DROP TABLE IF EXISTS {{ .Name }};
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
)

// DumpPostgres returns the SQL needed to recreate a Postgres database.
func (d *Dumper) DumpPostgres(filters ...string) error {
	list, err := d.GetPostgresTables(d.db)
	if err != nil {
		return err
	}

	fmt.Printf("%#v\n", list)
	return nil
}

// GetPostgresTables returns the table names from a PostgreSQL database.
func (d *Dumper) GetPostgresTables(db *sql.DB) ([]string, error) {
	tables := []string{}
	rows, err := db.Query(PG_SHOW_TABLES)
	if err != nil {
		return tables, err
	}

	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}
	return tables, rows.Err()
}
