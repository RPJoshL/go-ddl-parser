package ddl

import (
	"database/sql"
	"fmt"
	"strings"

	"git.rpjosh.de/RPJosh/go-logger"
)

type MariadbKeyType string

const (
	// Primary key
	MariadbKeyPrimary MariadbKeyType = "PRI"
	// Unique index
	MariadbKeyUnique MariadbKeyType = "UNI"
	// Nonunique index
	MariadbKeyMultipleIndex MariadbKeyType = "MUL"
)

var _ DbSystem = &Mariadb{}
var _ Columner = &MariadbColumn{}

// Mariadb implements "DbSystem" for a MariaDB database
type Mariadb struct {
	db *sql.DB
}

type MariadbColumn struct {
	*Column

	// Weather this column has the auto_increment flag
	AutoIncrement bool

	// The character lenght or numeric precision
	DataTypeLenght int

	// The internal column key like 'UNI' or 'PRI'
	KeyType MariadbKeyType
}

func (c *MariadbColumn) GetExtraInfos() string {
	return "MariaDB!"
}
func (c *MariadbColumn) GetSpecificInfos() any {
	return c
}
func (s *Mariadb) newColumn() *MariadbColumn {
	c := &MariadbColumn{}
	c.Column = &Column{}
	c.Column.Extras = c
	return c
}

// NewMariaDb initializes a new database parser for a MariaDB database
func NewMariaDb(db *sql.DB) DbSystem {
	return &Mariadb{
		db: db,
	}
}

func (s *Mariadb) GetTable(schema, name string) (*Table, error) {
	sql := `
		SELECT 
			c.TABLE_SCHEMA,
			c.TABLE_NAME,
			c.COLUMN_NAME,
			c.COLUMN_DEFAULT,
			c.IS_NULLABLE,
			c.DATA_TYPE,
			c.COLUMN_TYPE,
			COALESCE(c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.DATETIME_PRECISION, 0),
			c.COLUMN_KEY,
			c.COLUMN_COMMENT,
			c.extra,
			-- Foreign key data
			COALESCE(con.REFERENCED_TABLE_NAME, ''), COALESCE(con.REFERENCED_TABLE_SCHEMA, ''), COALESCE(con.REFERENCED_COLUMN_NAME, '')
  		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE con ON
			con.TABLE_NAME = c.TABLE_NAME AND con.TABLE_SCHEMA = c.TABLE_SCHEMA AND con.COLUMN_NAME = c.COLUMN_NAME 
				AND con.CONSTRAINT_NAME IN ( SELECT cc.CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS cc WHERE cc.TABLE_SCHEMA = c.TABLE_SCHEMA AND cc.TABLE_NAME = c.TABLE_NAME AND cc.CONSTRAINT_TYPE = 'FOREIGN KEY' )
	  	WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
	  	ORDER BY c.ordinal_position
	`
	rows, err := s.db.Query(sql, schema, name)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema: %s", err)
	}
	defer rows.Close()

	table := &Table{}
	count := 0
	for rows.Next() {
		var tableSchema, tableName, isNullable, dataType, extra string
		column := s.newColumn()

		if err := rows.Scan(
			&tableSchema, &tableName,
			&column.Name, &column.DefaultValue, &isNullable,
			&dataType, &column.InternalType, &column.DataTypeLenght,
			&column.KeyType, &column.Comment, &extra,
			&column.ForeignKeyColumn.Name, &column.ForeignKeyColumn.Schema, &column.ForeignKeyColumn.Column,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %s", err)
		}

		// Apply data
		column.CanBeNull = isNullable == "YES"
		column.Type = s.GetDataType(dataType)
		column.AutoIncrement = strings.Contains(extra, "auto_increment")
		column.PrimaryKey = column.KeyType == MariadbKeyPrimary
		column.ForeignKey = column.ForeignKeyColumn.Column != ""

		// The default value contains the raw single quotes of the create statement
		if column.DefaultValue.Valid {
			column.DefaultValue.String = strings.TrimPrefix(column.DefaultValue.String, "'")
			column.DefaultValue.String = strings.TrimSuffix(column.DefaultValue.String, "'")
		}

		// Initialize new table metadata
		if count == 0 {
			table.Schema = tableSchema
			table.Name = tableName
		}
		table.Columns = append(table.Columns, column.Column)
		count += 1
	}

	// We got no data
	if count == 0 {
		return nil, fmt.Errorf("%s.%s was not found", schema, name)
	}

	return table, nil
}

func (s *Mariadb) GetTables(schema string) ([]*Table, error) {
	sql := `
		SELECT
			t.TABLE_SCHEMA,
    		t.TABLE_NAME
		FROM information_schema.tables t 
		WHERE t.table_schema = ?
		ORDER BY t.TABLE_NAME ASC
	`
	rows, err := s.db.Query(sql, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema: %s", err)
	}
	defer rows.Close()

	rtc := []*Table{}
	for rows.Next() {
		var tableSchema, tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			return rtc, fmt.Errorf("failed to scan row: %s", err)
		}

		t, err := s.GetTable(tableSchema, tableName)
		if err != nil {
			return rtc, fmt.Errorf("failed to get data for %s.%s: %s", tableSchema, tableName, err)
		}
		rtc = append(rtc, t)
	}

	return rtc, nil
}

func (s *Mariadb) GetDataType(internalType string) DataType {
	switch strings.ToLower(internalType) {
	case "varchar", "text", "tinytext", "mediumtext", "longtext", "char":
		return StringType
	case "int", "tinyint", "smallint", "bigint":
		return IntType
	case "decimal", "number", "float", "double":
		return DoubleType
	case "datetime", "date":
		return DateType
	case "point":
		return GeoType
	default:
		logger.Warning("MariaDb: received unknown data type column: %s", internalType)
		return UnknownType
	}
}
