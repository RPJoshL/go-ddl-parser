package ddl

import (
	"database/sql"
	"fmt"
	"strings"

	"git.rpjosh.de/RPJosh/go-logger"
)

type OracleTableType string

const (
	OracleTable OracleTableType = "TABLE"
	OracleView  OracleTableType = "VIEW"
)

var _ DbSystem = &OracleDb{}
var _ Columner = &OracleColumn{}

// OracleDb implements "DbSystem" for an oracle database
type OracleDb struct {
	db *sql.DB
}

type OracleColumn struct {
	*Column

	// Weather this column has the auto_increment flag
	AutoIncrement bool

	// Character lenght or numeric precision on the LEFT side
	// of the dot
	DataTypeLenght int

	// Decimal precision on the RIGHT side of the dot
	Scale int
}

func (c *OracleColumn) GetExtraInfos() string {
	return "Oracle!"
}
func (c *OracleColumn) GetSpecificInfos() any {
	return c
}
func (s *OracleDb) newColumn() *OracleColumn {
	c := &OracleColumn{}
	c.Column = &Column{}
	c.Column.Extras = c
	return c
}

// NewMariaDb initializes a new database parser for an oracle database
func NewOracleDb(db *sql.DB) *OracleDb {
	return &OracleDb{
		db: db,
	}
}

func (s *OracleDb) GetTable(schema, name string) (*Table, error) {
	ssql := `
		SELECT 
			col.OWNER,
			col.table_name,
			col.COLUMN_NAME,
			col.DATA_DEFAULT,
			col.NULLABLE,
			col.DATA_TYPE,
			COALESCE(col.DATA_PRECISION, col.DATA_LENGTH, 0), COALESCE(col.DATA_SCALE, 0),
			col.IDENTITY_COLUMN, con.CONSTRAINT_TYPE, 
			coms.COMMENTS,
			-- Foreign key data
			act.OWNER, act.table_name, act.COLUMN_NAME
			FROM all_tab_columns col
			LEFT JOIN all_cons_columns cc ON cc.TABLE_NAME = col.TABLE_NAME AND col.COLUMN_NAME = cc.COLUMN_NAME
			LEFT JOIN all_constraints con ON cc.CONSTRAINT_NAME = con.CONSTRAINT_NAME
			LEFT JOIN all_cons_columns act ON con.r_owner = act.owner
	   			AND con.r_constraint_name = act.constraint_name
			LEFT JOIN dba_col_comments coms ON coms.OWNER = col.OWNER AND coms.TABLE_NAME = col.TABLE_NAME
				AND coms.COLUMN_NAME = col.COLUMN_NAME
			WHERE col.table_name = UPPER(:0)
	  			AND col.OWNER = UPPER(:1)
			ORDER BY col.column_id
	`
	rows, err := s.db.Query(ssql, name, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query all_tab_columns: %s", err)
	}
	defer rows.Close()

	lastColumnName := ""
	table := &Table{}
	count := 0
	for rows.Next() {
		var tableSchema, tableName, isNullable, identity string
		var fkOwner, fkTable, fkColumn, keyType, comment sql.NullString
		column := s.newColumn()

		if err := rows.Scan(
			&tableSchema, &tableName,
			&column.Name, &column.DefaultValue, &isNullable,
			&column.InternalType, &column.DataTypeLenght, &column.Scale,
			&identity, &keyType, &comment,
			&fkOwner, &fkTable, &fkColumn,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %s", err)
		}

		// Apply data
		column.CanBeNull = isNullable == "Y"
		column.Type = s.GetDataType(column.InternalType, column)
		column.PrimaryKey = identity == "YES" || (keyType.Valid && keyType.String == "P")
		if fkColumn.Valid {
			column.ForeignKey = true
			column.ForeignKeyColumn.Column = fkColumn.String
			column.ForeignKeyColumn.Name = fkTable.String
			column.ForeignKeyColumn.Schema = fkOwner.String
		}

		// The default value contains the raw single quotes of the create statement
		if column.DefaultValue.Valid {
			column.DefaultValue.String = strings.TrimPrefix(column.DefaultValue.String, "'")
			column.DefaultValue.String = strings.TrimSuffix(column.DefaultValue.String, "'")
		}

		// Set comment
		if comment.Valid {
			column.Comment = strings.ReplaceAll(comment.String, "\\n", "\n")
		}

		// Initialize new table metadata
		if count == 0 {
			table.Schema = tableSchema
			table.Name = tableName
		}

		// It's possible that we get the same column twice for different keyTypes.
		// Always prefer the primary or foreign key constraint
		if lastColumnName == column.Name {
			if column.ForeignKey || column.PrimaryKey {
				// Don't skip, but remove the last one
				table.Columns = table.Columns[:len(table.Columns)-1]
			} else {
				// We use the primary key or foreign key
				continue
			}
		}
		lastColumnName = column.Name

		table.Columns = append(table.Columns, column.Column)
		count += 1
	}

	// We got no data
	if count == 0 {
		return nil, fmt.Errorf("%s.%s was not found", schema, name)
	}

	return table, nil
}

func (s *OracleDb) GetTables(schema string) ([]*Table, error) {
	return s.GetTablesByType(schema, OracleTable)
}

func (s *OracleDb) GetTablesByType(schema string, typ OracleTableType) ([]*Table, error) {
	sql := `
		SELECT DISTINCT
			OWNER,
    		OBJECT_NAME,
			OBJECT_TYPE
		FROM ALL_OBJECTS
		WHERE  OBJECT_TYPE = :0
		   AND OWNER <> 'SYS' 
		   AND OWNER = :1
		ORDER BY OBJECT_NAME ASC
	`
	rows, err := s.db.Query(sql, string(typ), schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query all_objects: %s", err)
	}
	defer rows.Close()

	rtc := []*Table{}
	for rows.Next() {
		var tableSchema, tableName, tableType string
		if err := rows.Scan(&tableSchema, &tableName, &tableType); err != nil {
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

func (s *OracleDb) GetDataType(internalType string, col *OracleColumn) DataType {
	internalType = strings.ToLower(internalType)

	// Remove any data type length (for some datatypes they are returned...)
	if lastBracket := strings.Index(internalType, "("); lastBracket != -1 {
		internalType = internalType[:lastBracket]
	}

	switch internalType {
	case "varchar", "varchar2", "nvarchar", "nvarchar2":
		return StringType
	case "double":
		return DoubleType
	case "date", "timestamp", "timestamptz":
		return DateType
	default:
		// A number can either be a double or a int
		if internalType == "number" {
			if col.Scale == 0 {
				return IntType
			} else {
				return DoubleType
			}
		}
		logger.Warning("OracleDb: received unknown data type column: %s", internalType)
		return UnknownType
	}
}
