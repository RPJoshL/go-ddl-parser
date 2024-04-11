package structt

import (
	"fmt"
	"strings"

	"git.rpjosh.de/RPJosh/go-ddl-parser"

	"git.rpjosh.de/RPJosh/go-logger"
)

// Identifier of the struct tag for "ColumnTag"
const ColumnTagId = "dbColumn"

type Relationship int

const (
	OneToOne Relationship = iota
	OneToMany
	ManyToMany
)

// ColumnTag contains information that are stored via a
// struct tag for a column.
type ColumnTag struct {

	// Name of the column in the database
	Name string

	// Weather this field is a primary key
	IsPrimaryKey bool

	// Column to which this column points to via a foreign key in
	// format Schema.Table.Column
	ForeignKeyReference string

	// Column from which this struct was referenced (n:1 relations) in
	// format Schema.Table.Column.
	// If this field is present, all other fields are empty
	PointedKeyReference string

	// Mariadb: weather this field has the property "auto_increment"
	AutoIncrement bool
}

// Identifier of the struct tag for "MetadataTag"
const MetadataTagId = "dbMetadata"
const MetadataFieldName = "DbMetadata_"

// MetadataTag is a generic field that is added to every struct to store some
// metadata about the table.
// It's used so we do not need to fetch the table structure during runtime
type MetadataTag struct {

	// Schema the table belongs to
	Schema string

	// Name of the table this struct represents
	Table string
}

// GetColumnTag returns a "ColumnTag" struct from a ddl column
func GetColumnTag(col *ddl.Column) *ColumnTag {
	rtc := &ColumnTag{
		Name:         col.Name,
		IsPrimaryKey: col.PrimaryKey,
	}

	// Add foreign key
	if col.ForeignKey {
		// Schema is optional
		if col.ForeignKeyColumn.Schema != "" {
			rtc.ForeignKeyReference = col.ForeignKeyColumn.Schema + "."
		}

		rtc.ForeignKeyReference += col.ForeignKeyColumn.Name + "." + col.ForeignKeyColumn.Column
	}

	// Add auto increment property
	if mariaDb, ok := col.Extras.(*ddl.MariadbColumn); ok {
		rtc.AutoIncrement = mariaDb.AutoIncrement
	}

	return rtc
}

// ToTag transforms this columnTag to a string that can be applied as
// struct tag
func (c *ColumnTag) ToTag() string {
	rtc := "Column:" + c.Name

	if c.AutoIncrement {
		rtc += ",AutoIncrement"
	}
	if c.IsPrimaryKey {
		rtc += ",PrimaryKey"
	}
	if c.ForeignKeyReference != "" {
		rtc += ",ForeignKey:" + c.ForeignKeyReference
	}
	if c.PointedKeyReference != "" {
		rtc += ",PointedForeignKey:" + c.PointedKeyReference
	}

	return rtc
}

// FromColumnTag transforms a struct tag containing a "ColumnTag" to the
// represended struct
func FromColumnTag(tag string) *ColumnTag {
	rtc := &ColumnTag{}

	// Valus are seperated by ","
	vals := strings.Split(tag, ",")
	for _, val := range vals {
		// Boolean flags
		switch val {
		case "AutoIncrement":
			rtc.AutoIncrement = true
		case "PrimaryKey":
			rtc.IsPrimaryKey = true
		}

		// Key-value pairs
		if strings.Contains(val, ":") {
			point := strings.Index(val, ":")
			key := val[0:point]

			// No value specified
			if point+1 == len(val) {
				logger.Warning("No value specified for column tag %q", val)
				continue
			}
			value := val[point+1:]

			switch key {
			case "Column":
				rtc.Name = value
			case "ForeignKey":
				rtc.ForeignKeyReference = value
			case "PointedForeignKey":
				rtc.PointedKeyReference = value
			default:
				logger.Warning("Unknown key %q specified for column tag", key)
			}
		}
	}

	return rtc
}

// ToTag transforms this columnTag to a string that can be applied as
// struct tag
func (c *MetadataTag) ToTag() string {
	rtc := fmt.Sprintf("Schema:%s,Table:%s", c.Schema, c.Table)

	return rtc
}

// FromMetadataTag transforms a struct tag containing a "MetadataTag" to the
// represended struct
func FromMetadataTag(tag string) *MetadataTag {
	rtc := &MetadataTag{}

	// Valus are seperated by ","
	vals := strings.Split(tag, ",")
	for _, val := range vals {

		// Key-value pairs
		if strings.Contains(val, ":") {
			point := strings.Index(val, ":")
			key := val[0:point]

			// No value specified
			if point+1 == len(val) {
				logger.Warning("No value specified for metadata tag %q", val)
				continue
			}
			value := val[point+1:]

			switch key {
			case "Schema":
				rtc.Schema = value
			case "Table":
				rtc.Table = value
			default:
				logger.Warning("Unknown key %q specified for column tag", key)
			}
		}
	}

	return rtc
}
