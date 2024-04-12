package ddl

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"git.rpjosh.de/RPJosh/go-logger"
	"github.com/google/go-cmp/cmp"
	goOra "github.com/sijms/go-ora/v2"
)

// TestGetTableSimple tests the construction of a Table struct
// with all supported data types and fields
func TestGetTableSimpleOracle(t *testing.T) {
	db := ConnectToOracle(t)
	oDb := NewOracleDb(db)

	// Create test table
	tableName, err := createTable(db,
		`
		id 		NUMERIC(10,0) PRIMARY KEY NOT NULL,
		txt 	VARCHAR2(100) DEFAULT 'Ich bins, der Tim!',
		dte		DATE NOT NULL
		`,
	)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	tableName = strings.ToUpper(tableName)
	defer dropTable(db, tableName)

	// Comment table
	if err := addOracleComment(db, tableName, "DTE", `Hallo ihr da!\nZeilenumbrüche`); err != nil {
		t.Fatalf("Failed to comment table: %s", err)
	}

	// Get columns
	table, err := oDb.GetTable(RequireEnvString("ORACLE_USER", t), tableName)
	if err != nil {
		t.Fatalf("Failed to get columns: %s", err)
	}

	expected := &Table{
		Name:   strings.ToUpper(tableName),
		Schema: RequireEnvString("ORACLE_USER", t),
	}
	columns := []*OracleColumn{
		{
			Column: &Column{
				Name:         "ID",
				PrimaryKey:   true,
				CanBeNull:    false,
				Type:         IntType,
				InternalType: "NUMBER",
			},
			DataTypeLenght: 10,
			Scale:          0,
		},
		{
			Column: &Column{
				Name:         "TXT",
				PrimaryKey:   false,
				CanBeNull:    true,
				Type:         StringType,
				InternalType: "VARCHAR2",
				DefaultValue: sql.NullString{
					Valid:  true,
					String: "Ich bins, der Tim!",
				},
			},
			AutoIncrement:  false,
			DataTypeLenght: 100,
		},
		{
			Column: &Column{
				Name:         "DTE",
				PrimaryKey:   false,
				CanBeNull:    false,
				Type:         DateType,
				InternalType: "DATE",
				Comment:      "Hallo ihr da!\nZeilenumbrüche",
			},
			AutoIncrement:  false,
			DataTypeLenght: 7,
		},
	}
	for _, c := range columns {
		c.Extras = c
		expected.Columns = append(expected.Columns, c.Column)
	}

	// Compare struct
	if diff := cmp.Diff(table, expected); diff != "" {
		t.Errorf("Mismatch of columns (-want +got):\n%s", diff)
	}
}

// TestGetTableSimple tests the construction of a Table struct
// that references another table
func TestGetTableOracleFK(t *testing.T) {
	db := ConnectToOracle(t)
	mDb := NewOracleDb(db)

	// Create table we reference to
	referenceTableName, err := createTable(db, `
		id_to_ref   NUMBER(10,0) PRIMARY KEY NOT NULL,
		rand        VARCHAR2(10) NOT NULL`,
	)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropTable(db, referenceTableName)

	// Create table with reference
	tableName, err := createTable(db, `
		id 		 NUMBER(10,0) PRIMARY KEY NOT NULL,
		other_id NUMBER(10,0) NOT NULL,
		CONSTRAINT fk_test_constraint_for_you FOREIGN KEY(other_id) REFERENCES `+referenceTableName+`(id_to_ref)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropTable(db, tableName)

	table, err := mDb.GetTable(RequireEnvString("ORACLE_USER", t), tableName)
	if err != nil {
		t.Fatalf("Failed to get columns: %s", err)
	}

	expected := &Table{
		Name:   strings.ToUpper(tableName),
		Schema: RequireEnvString("ORACLE_USER", t),
	}
	columns := []*OracleColumn{
		{
			Column: &Column{
				Name:         "ID",
				PrimaryKey:   true,
				CanBeNull:    false,
				Type:         IntType,
				InternalType: "NUMBER",
			},
			DataTypeLenght: 10,
		},
		{
			Column: &Column{
				Name:         "OTHER_ID",
				PrimaryKey:   false,
				CanBeNull:    false,
				Type:         IntType,
				InternalType: "NUMBER",
				ForeignKey:   true,
				ForeignKeyColumn: ForeignColumn{
					Name:   strings.ToUpper(referenceTableName),
					Schema: RequireEnvString("ORACLE_USER", t),
					Column: "ID_TO_REF",
				},
			},
			DataTypeLenght: 10,
		},
	}
	for _, c := range columns {
		c.Extras = c
		expected.Columns = append(expected.Columns, c.Column)
	}

	// Compare struct
	if diff := cmp.Diff(table, expected); diff != "" {
		t.Errorf("Mismatch (-want +got):\n%s", diff)
	}

}

// TestGetTableSimple tests the selecting of multiple tables to a []Table array
func TestGetTablesOracle(t *testing.T) {
	db := ConnectToOracle(t)
	mDb := NewOracleDb(db)

	// Create two simple tables
	tableName1, err := createTable(db, `idTab1 NUMBER(10,0) NOT NULL`)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	tableName1 = strings.ToUpper(tableName1)
	defer dropTable(db, tableName1)

	tableName2, err := createTable(db, `idTab2 NUMBER(10,0) NOT NULL`)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	tableName2 = strings.ToUpper(tableName2)
	defer dropTable(db, tableName2)

	tables, err := mDb.GetTables(RequireEnvString("ORACLE_USER", t))
	if err != nil {
		t.Fatalf("Failed to get tables: %s", err)
	}

	found1 := 0
	found2 := 0
	for _, tt := range tables {
		if tt.Name == tableName1 {
			found1 = found1 + 1

			// Compare table
			expected := &Table{
				Name:   tt.Name,
				Schema: RequireEnvString("ORACLE_USER", t),
			}
			columns := []*OracleColumn{
				{
					Column: &Column{
						Name:         "IDTAB1",
						CanBeNull:    false,
						Type:         IntType,
						InternalType: "NUMBER",
					},
					DataTypeLenght: 10,
				},
			}
			for _, c := range columns {
				c.Extras = c
				expected.Columns = append(expected.Columns, c.Column)
			}

			// Compare struct
			if diff := cmp.Diff(tt, expected); diff != "" {
				t.Errorf("TestGetTables() mismatch of tab1: (-want +got):\n%s", diff)
			}
		}

		if tt.Name == tableName2 {
			found2 = found2 + 1

			// Compare table
			expected := &Table{
				Name:   tt.Name,
				Schema: RequireEnvString("ORACLE_USER", t),
			}
			columns := []*OracleColumn{
				{
					Column: &Column{
						Name:         "IDTAB2",
						CanBeNull:    false,
						Type:         IntType,
						InternalType: "NUMBER",
					},
					DataTypeLenght: 10,
				},
			}
			for _, c := range columns {
				c.Extras = c
				expected.Columns = append(expected.Columns, c.Column)
			}

			// Compare struct
			if diff := cmp.Diff(tt, expected); diff != "" {
				t.Errorf("Mismatch of tab2: (-want +got):\n%s", diff)
			}
		}
	}

	// We expected to find exactly one single table
	if found1 != 1 {
		t.Errorf("Found %d instances of tab1. Expected 1 (len(rtc) = %d)", found1, len(tables))
	}
	if found2 != 1 {
		t.Errorf("Found %d instances of tab2. Expected 1 (len(rtc) = %d)", found1, len(tables))
	}
}

func addOracleComment(db *sql.DB, tbl string, column string, comment string) error {
	comment = strings.ReplaceAll(comment, "\n", `'||char(10)||'`)
	sql := fmt.Sprintf("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tbl, column, comment)
	_, err := db.Exec(sql)
	if err != nil {
		logger.Debug("Statement for create comment:\n%s", sql)
	}
	return err
}

func ConnectToOracle(t *testing.T) *sql.DB {
	conString := goOra.BuildUrl(
		RequireEnvString("ORACLE_SERVER", t),
		RequireEnvInt("ORACLE_PORT", t),
		RequireEnvString("ORACLE_SERVICE", t),
		RequireEnvString("ORACLE_USER", t),
		RequireEnvString("ORACLE_PASSWORD", t),
		map[string]string{},
	)

	db, err := sql.Open("oracle", conString)
	if err != nil {
		panic(fmt.Sprintf("Failed to open DB connection: %s", err))
	}

	return db
}
