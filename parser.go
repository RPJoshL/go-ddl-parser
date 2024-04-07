package ddl

// DbSystem has to be implemented by every single database system that is
// supported by the module to fetch a list of tables and columns
type DbSystem interface {

	// GetTable returns a single table identified by the schema and name
	GetTable(schema, name string) (*Table, error)

	// GeTables returns a list of tables that are present in the provided schema
	// or database
	GetTables(schema string) ([]*Table, error)
}
