package ddl

import "database/sql"

// DataType is a generic data type of a db type
type DataType string

const (
	StringType  DataType = "String"
	IntType     DataType = "Integer"
	DoubleType  DataType = "Double"
	DateType    DataType = "Date"
	GeoType     DataType = "Geo"
	UnknownType DataType = "Unknown"
)

// Table represents a logical table on the database
type Table struct {

	// Name of the table
	Name string

	// Schema or database the table belongs to
	Schema string

	// List of columns the table has
	Columns []*Column
}

// Column of a table.
// Every database system has a own column struct that embeds
// this type and extends it with database specific informations
type Column struct {

	// Unique column name within a table
	Name string

	// Generic data type definition
	Type DataType

	// Internal name of the data type (with lenght)
	InternalType string

	// Weather this column is a primary key of the table
	PrimaryKey bool

	// Weather this column references another table
	ForeignKey bool
	// Table to which this column has a reference to
	ForeignKeyColumn ForeignColumn

	// Weather this column can be null
	CanBeNull bool

	// A default value of the column
	DefaultValue sql.NullString

	// Comment of this column
	Comment string

	// Extras
	Extras Columner
}

// Columner returns additonal informations to a column that are specific for a SQL system
type Columner interface {

	// GetExtraInfos returns a string with additional properties that is passed
	// to the struct tag "Db" when using the struct generator
	GetExtraInfos() string

	// GetSpecificInfos returns the underlaying
	GetSpecificInfos() any
}

// ForeignColumn contains information to which the column points to with an foreign key
type ForeignColumn struct {

	// Name of the referenced table
	Name string

	// Schema or database the table belongs to
	Schema string

	// Name of the column that is referenced
	Column string
}
