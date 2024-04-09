package structt

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"git.rpjosh.de/RPJosh/go-ddl-parser"

	"github.com/google/go-cmp/cmp"
)

func TestFindTableconfigSchema(t *testing.T) {
	tbl := &ddl.Table{
		Name:   "tbl",
		Schema: "workout",
	}
	configGeneric := &TableConfig{
		Path: "Some value for GENERIC...",
	}
	configSpecific := &TableConfig{
		Path: "Some value for SPECIFIC...",
	}

	c := &constructor{
		config: &StructConfig{
			Tableconfig: map[string]*TableConfig{
				tbl.Name:                    configGeneric,
				tbl.Schema + "." + tbl.Name: configSpecific,
			},
		},
	}

	// Compare structs
	if diff := cmp.Diff(configSpecific, c.findTableConfig(tbl)); diff != "" {
		t.Errorf("TestFindTableconfig() mismatch (-want +got):\n%s", diff)
	}
}

func TestFindTableconfigTable(t *testing.T) {
	tbl := &ddl.Table{
		Name:   "tbl",
		Schema: "workout",
	}
	configGeneric := &TableConfig{
		Path: "Some value for GENERIC...",
	}
	c := &constructor{
		config: &StructConfig{
			Tableconfig: map[string]*TableConfig{
				tbl.Name: configGeneric,
			},
		},
	}

	// Compare structs
	if diff := cmp.Diff(configGeneric, c.findTableConfig(tbl)); diff != "" {
		t.Errorf("TestFindTableconfig() mismatch (-want +got):\n%s", diff)
	}
}

func TestGetGoFileSimple(t *testing.T) {
	testGetGoFileSimple(t)
}

func testGetGoFileSimple(t *testing.T) (expected string, tableConfig *TableConfig, table *ddl.Table) {
	c := &constructor{
		config: &StructConfig{},
	}

	table = &ddl.Table{
		Name:   "my_table_name",
		Schema: "here_is_me",
		Columns: []*ddl.Column{
			{
				Name:    "id",
				Type:    ddl.IntType,
				Comment: "It's me, mario\nAnd it's cool!",
			},
			{
				Name:      "with_under",
				Type:      ddl.StringType,
				CanBeNull: true,
			},
		},
	}
	tableConfig = &TableConfig{
		PackageName: "olaf",
		Suffix:      "Tab",
	}

	expected =
		`package olaf

import (
	"database/sql"
)

type MyTableNameTab struct {
	// It's me, mario
	// And it's cool!
	Id int ` + getStructTag(table.Columns[0]) + `
	WithUnder sql.NullString ` + getStructTag(table.Columns[1]) + `
	` + MetadataFieldName + ` any ` + getMetadataTag(table) + `
}
// MyTableNameTab
const (
	MyTableNameTab_Id string = "Id"
	MyTableNameTab_WithUnder string = "WithUnder"
)
`
	goFile := c.getGoFile("", table, tableConfig)

	// Compare structs
	if diff := cmp.Diff(
		replaceWhitespaces(expected),
		replaceWhitespaces(goFile),
	); diff != "" {
		t.Errorf("TestGetGoFileSimple() mismatch (-want +got):\n%s", diff)
		t.Logf("Expected:\n%s", expected)
		t.Logf("Actual:\n%s", goFile)
	}

	return
}

// Test the building of go files with a foreign key reference (1:1)
func TestRelationshipOneToOne(t *testing.T) {

	tableConfig1 := &TableConfig{
		PackageName:              "olaf",
		Suffix:                   "Tab",
		IncludeReferencedStructs: []string{"*"},
	}
	tableConfig2 := &TableConfig{
		PackageName: "olaf",
		Suffix:      "Tab",
	}

	tables := []*ddl.Table{
		{
			Name:   "my_table_name",
			Schema: "here_is_me",
			Columns: []*ddl.Column{
				{
					Name: "id",
					Type: ddl.IntType,
				},
				{
					Name:       "user_id",
					Type:       ddl.IntType,
					ForeignKey: true,
					ForeignKeyColumn: ddl.ForeignColumn{
						Name:   "user_reference",
						Column: "id",
						Schema: "here_is_me",
					},
				},
			},
		},
		{
			Name:   "user_reference",
			Schema: "here_is_me",
			Columns: []*ddl.Column{
				{
					Name:       "id",
					Type:       ddl.IntType,
					PrimaryKey: true,
				},
			},
		},
	}
	c := &constructor{
		config: &StructConfig{
			Tableconfig: map[string]*TableConfig{
				"my_table_name":  tableConfig1,
				"user_reference": tableConfig2,
			},
		},
		tables: tables,
	}

	// Expecting 1:1 reference to struct
	tags := GetColumnTag(tables[0].Columns[1])
	dt, _ := c.getDataType(tables[0].Columns[1], tableConfig1, tags)
	if dt != "*UserReferenceTab" {
		t.Errorf("Expected 1:1 reference '*UserReferenceTab'. Found '%s'", dt)
	}
	expectedTag := "here_is_me.user_reference.id"
	if tags.ForeignKeyReference != expectedTag {
		t.Errorf("Expected tag of foreign key (1:1) reference to be '%s'. Found '%s'", expectedTag, tags.ForeignKeyReference)
	}

	// Do not use 1:1 if not configured
	tableConfig1.IncludeReferencedStructs = []string{"some_random"}
	tags = GetColumnTag(tables[0].Columns[1])
	dt, _ = c.getDataType(tables[0].Columns[1], tableConfig1, tags)
	if dt != "int" {
		t.Errorf("Expected no 1:1 reference 'int'. Found '%s'", dt)
	}
}

// Test the building of go files with a foreign key reference (n:1)
func TestRelationshipOneToMany(t *testing.T) {

	tableConfig1 := &TableConfig{
		PackageName: "olaf",
		Suffix:      "Tab",
	}
	tableConfig2 := &TableConfig{
		PackageName:           "olaf",
		Suffix:                "Tab",
		IncludePointedStructs: true,
	}

	tables := []*ddl.Table{
		{
			Name:   "workout_details",
			Schema: "here_is_me",
			Columns: []*ddl.Column{
				{
					Name: "id",
					Type: ddl.IntType,
				},
				{
					Name:       "workout_id",
					Type:       ddl.IntType,
					ForeignKey: true,
					ForeignKeyColumn: ddl.ForeignColumn{
						Name:   "workout",
						Column: "id",
						Schema: "here_is_me",
					},
				},
			},
		},
		{
			Name:   "workout",
			Schema: "here_is_me",
			Columns: []*ddl.Column{
				{
					Name:       "id",
					Type:       ddl.IntType,
					PrimaryKey: true,
				},
			},
		},
	}
	c := &constructor{
		config: &StructConfig{
			Tableconfig: map[string]*TableConfig{
				"workout_details": tableConfig1,
				"workout":         tableConfig2,
			},
		},
		tables: tables,
	}

	// Expecting 1:1 reference to struct
	dt, _ := c.getOneToMany(tableConfig2, tables[1])
	expectedTag := &ColumnTag{
		PointedKeyReference: "here_is_me.workout_details.workout_id",
	}
	expected := fmt.Sprintf("\tWorkoutDetails []*WorkoutDetailsTab `%s:\"%s\"`\n", ColumnTagId, expectedTag.ToTag())

	// Compare structs
	if diff := cmp.Diff(
		replaceWhitespaces(expected),
		replaceWhitespaces(dt),
	); diff != "" {
		t.Errorf("TestRelationshipOneToMany() mismatch (-want +got):\n%s", diff)
		t.Logf("Expected:\n%s", expected)
		t.Logf("Actual:\n%s", dt)
	}
}

func TestPatchFileAppend(t *testing.T) {

	existingContent := `
package olaf

import "time"

type SomeRandom struct {
	` + MetadataFieldName + ` any
}`
	expected := `
package olaf

import (
	"sql.NullString"
	"time"
)

type SomeRandom struct {
	` + MetadataFieldName + ` any
}

>>>NewContent<<<`

	c := &constructor{
		config: &StructConfig{},
	}

	newcontent := c.patchFile(existingContent, ">>>NewContent<<<", &ddl.Table{
		Schema: "schema",
		Name:   "table",
	}, &TableConfig{}, map[string]bool{"sql.NullString": true})

	// Compare structs
	if diff := cmp.Diff(
		replaceWhitespaces(expected),
		replaceWhitespaces(newcontent),
	); diff != "" {
		t.Errorf("TestGetGoFileSimple() mismatch (-want +got):\n%s", diff)
		t.Logf("Expected:\n%s", expected)
		t.Logf("Actual:\n%s", newcontent)
	}

}

func TestPatchFilePatch(t *testing.T) {

	existingContent := `
package olaf

import (
	"time"
	"database/sql"
)

type SomeRandomTab struct {
	// Existing content
	` + MetadataFieldName + ` any
}
// SomeRandomTab
const (
	SomeRandomTab_Col1 string  = ""
	SomeRandomTab_Col2 string = ""
)

type ItsHere struct {
	` + MetadataFieldName + ` any
}
`
	expected := `
package olaf

import (
	"database/sql"
	"sql.NullString"
	"time"
)

>>>NewContent<<<

type ItsHere struct {
	` + MetadataFieldName + ` any
}
`

	c := &constructor{
		config: &StructConfig{},
	}

	newcontent := c.patchFile(existingContent, ">>>NewContent<<<", &ddl.Table{
		Schema: "schema",
		Name:   "some_random_tab",
	}, &TableConfig{}, map[string]bool{"sql.NullString": true})

	// Compare structs
	if diff := cmp.Diff(
		replaceWhitespaces(expected),
		replaceWhitespaces(newcontent),
	); diff != "" {
		t.Errorf("TestGetGoFileSimple() mismatch (-want +got):\n%s", diff)
		t.Logf("Expected:\n%s", expected)
		t.Logf("Actual:\n%s", newcontent)
	}

}

// Tests weather a go file created by this module can be patched by itself!
func TestPatchFilePatchSelf(t *testing.T) {
	c := &constructor{
		config: &StructConfig{},
	}

	// Use previous test case
	expected, conf, tbl := testGetGoFileSimple(t)

	// Replace a single column ID to test some change.
	// WithUnder -> ReplacedColumn
	expected = strings.ReplaceAll(expected, "WithUnder", "ReplacedColumn")
	expected = strings.ReplaceAll(expected, "withUnder", "replacedColumn")
	expected = strings.ReplaceAll(expected, "with_under", "replaced_column")
	tbl.Columns[1].Name = "replaced_column"

	goFile := c.getGoFile("", tbl, conf)

	// Compare structs
	if diff := cmp.Diff(
		replaceWhitespaces(expected),
		replaceWhitespaces(goFile),
	); diff != "" {
		t.Errorf("TestGetGoFileSimple() mismatch (-want +got):\n%s", diff)
		t.Logf("Expected:\n%s", expected)
		t.Logf("Actual:\n%s", goFile)
	}
}

// replaceWhitespaces replaces any space, newline or a squecne of
// spaces with a single space
func replaceWhitespaces(val string) string {
	re := regexp.MustCompile(`[\t\r\v\f ]+|\n{2,}`)
	rtc := re.ReplaceAllStringFunc(val, func(match string) string {
		if match == "\n\n" {
			return "\n"
		}
		if match == "\n" {
			return "\n"
		}
		return " "
	})

	return rtc
}

// getStructTag returns the tag for a struct to append to.
// We already tested the struct tags in another test!
func getStructTag(col *ddl.Column) string {
	tagStart := "`json:\"" + GetJsonName(col.Name) + "\" " + ColumnTagId + ":\""
	tagEnd := "\"`"

	return tagStart + GetColumnTag(col).ToTag() + tagEnd
}
func getMetadataTag(tbl *ddl.Table) string {
	tagStart := "`json:\"-\" " + MetadataTagId + ":\""
	tagEnd := "\"`"

	m := &MetadataTag{
		Schema: tbl.Schema,
		Table:  tbl.Name,
	}
	return tagStart + m.ToTag() + tagEnd
}
