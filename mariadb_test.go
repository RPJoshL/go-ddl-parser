package ddl

import (
	"database/sql"
	"fmt"
	"testing"

	"git.rpjosh.de/RPJosh/go-logger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
)

// TestGetTableSimple tests the construction of a Table struct
// with all supported data types and fields
func TestGetTableSimple(t *testing.T) {
	db := ConnectToMariadb(t)
	mDb := NewMariaDb(db)

	// Create test table
	tableName, err := createMariadbTable(db,
		`
		id 		INT(10) PRIMARY KEY NOT NULL AUTO_INCREMENT,
		txt 	VARCHAR(100) DEFAULT 'Ich bins, der Tim!',
		dte		DATETIME NOT NULL
			COMMENT 'Hallo ihr da!\nZeilenumbrüche'
		`,
	)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropMariadbTable(db, tableName)

	// Get columns
	table, err := mDb.GetTable(RequireEnvString("MARIADB_DB", t), tableName)
	if err != nil {
		t.Fatalf("Failed to get columns: %s", err)
	}

	expected := &Table{
		Name:   tableName,
		Schema: RequireEnvString("MARIADB_DB", t),
	}
	columns := []*MariadbColumn{
		{
			Column: &Column{
				Name:         "id",
				PrimaryKey:   true,
				CanBeNull:    false,
				Type:         IntType,
				InternalType: "int(10)",
			},
			AutoIncrement:  true,
			DataTypeLenght: 10,
			KeyType:        MariadbKeyPrimary,
		},
		{
			Column: &Column{
				Name:         "txt",
				PrimaryKey:   false,
				CanBeNull:    true,
				Type:         StringType,
				InternalType: "varchar(100)",
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
				Name:         "dte",
				PrimaryKey:   false,
				CanBeNull:    false,
				Type:         DateType,
				InternalType: "datetime",
				Comment:      "Hallo ihr da!\nZeilenumbrüche",
			},
			AutoIncrement:  false,
			DataTypeLenght: 0,
		},
	}
	for _, c := range columns {
		c.Extras = c
		expected.Columns = append(expected.Columns, c.Column)
	}

	// Compare struct
	if diff := cmp.Diff(table, expected); diff != "" {
		t.Errorf("TestGetTable() mismatch (-want +got):\n%s", diff)
	}
}

// TestGetTableSimple tests the construction of a Table struct
// that references another table
func TestGetTableFK(t *testing.T) {
	db := ConnectToMariadb(t)
	mDb := NewMariaDb(db)

	// Create table we reference to
	referenceTableName, err := createMariadbTable(db, `
		id_to_ref   INT(10) PRIMARY KEY NOT NULL AUTO_INCREMENT,
		rand        VARCHAR(10) NOT NULL`,
	)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropMariadbTable(db, referenceTableName)

	// Create table with reference
	tableName, err := createMariadbTable(db, `
		id 		 INT(10) PRIMARY KEY NOT NULL AUTO_INCREMENT,
		other_id INT(10) NOT NULL,
		CONSTRAINT fk_test_constraint_for_you FOREIGN KEY(other_id) REFERENCES `+referenceTableName+`(id_to_ref)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropMariadbTable(db, tableName)

	table, err := mDb.GetTable(RequireEnvString("MARIADB_DB", t), tableName)
	if err != nil {
		t.Fatalf("Failed to get columns: %s", err)
	}

	expected := &Table{
		Name:   tableName,
		Schema: RequireEnvString("MARIADB_DB", t),
	}
	columns := []*MariadbColumn{
		{
			Column: &Column{
				Name:         "id",
				PrimaryKey:   true,
				CanBeNull:    false,
				Type:         IntType,
				InternalType: "int(10)",
			},
			AutoIncrement:  true,
			DataTypeLenght: 10,
			KeyType:        MariadbKeyPrimary,
		},
		{
			Column: &Column{
				Name:         "other_id",
				PrimaryKey:   false,
				CanBeNull:    false,
				Type:         IntType,
				InternalType: "int(10)",
				ForeignKey:   true,
				ForeignKeyColumn: ForeignColumn{
					Name:   referenceTableName,
					Schema: RequireEnvString("MARIADB_DB", t),
					Column: "id_to_ref",
				},
			},
			DataTypeLenght: 10,
			KeyType:        MariadbKeyMultipleIndex,
		},
	}
	for _, c := range columns {
		c.Extras = c
		expected.Columns = append(expected.Columns, c.Column)
	}

	// Compare struct
	if diff := cmp.Diff(table, expected); diff != "" {
		t.Errorf("TestGetTableFK() mismatch (-want +got):\n%s", diff)
	}

}

// TestGetTableSimple tests the selecting of multiple tables to a []Table array
func TestGetTables(t *testing.T) {
	db := ConnectToMariadb(t)
	mDb := NewMariaDb(db)

	// Create two simple tables
	tableName1, err := createMariadbTable(db, `idTab1 INT(10) NOT NULL`)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropMariadbTable(db, tableName1)

	tableName2, err := createMariadbTable(db, `idTab2 INT(10) NOT NULL`)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	defer dropMariadbTable(db, tableName2)

	tables, err := mDb.GetTables(RequireEnvString("MARIADB_DB", t))
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
				Schema: RequireEnvString("MARIADB_DB", t),
			}
			columns := []*MariadbColumn{
				{
					Column: &Column{
						Name:         "idTab1",
						CanBeNull:    false,
						Type:         IntType,
						InternalType: "int(10)",
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
				Schema: RequireEnvString("MARIADB_DB", t),
			}
			columns := []*MariadbColumn{
				{
					Column: &Column{
						Name:         "idTab2",
						CanBeNull:    false,
						Type:         IntType,
						InternalType: "int(10)",
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
				t.Errorf("TestGetTables() mismatch of tab2: (-want +got):\n%s", diff)
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

func ConnectToMariadb(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		RequireEnvString("MARIADB_USER", t), RequireEnvString("MARIADB_PASSWORD", t), RequireEnvString("MARIADB_ADDRESS", t), RequireEnvString("MARIADB_DB", t),
	))
	if err != nil {
		panic(fmt.Sprintf("Failed to open DB connection: %s", err))
	}

	return db
}

// createMariadbTable creates a table with the provided column configuration
// in statementand returns the created table name
func createMariadbTable(db *sql.DB, statement string) (string, error) {
	name, _ := GenerateRandomString(8)
	name = "ddl_test_" + name
	sql := fmt.Sprintf("CREATE TABLE %s (%s)", name, statement)

	_, err := db.Exec(sql)
	if err != nil {
		logger.Debug("Create statement: %s", sql)
	}
	return name, err
}
func dropMariadbTable(db *sql.DB, tableName string) error {
	_, err := db.Exec("DROP TABLE " + tableName)
	return err
}
