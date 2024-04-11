package structt

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"git.rpjosh.de/RPJosh/go-ddl-parser"

	"git.rpjosh.de/RPJosh/go-logger"
)

// StructConfig contains options used to customize the behaviour of the conversion
// from a database column to a struct
type StructConfig struct {

	// Absolute or relative base path to write all files to:
	// '/internal/modules/'
	GenericOutputPath string `yaml:"genericOutputPath"`

	// Name of the Go package used for new files
	PackgeName string `yaml:"packageName"`

	// Suffix to add to the struct name for every table
	Suffix string `yaml:"suffix"`

	// Configuration options for a specific table.
	// The key of this map is either the table name (for any schema)
	// or a combination of "schema.tableName"
	Tableconfig map[string]*TableConfig `yaml:"tableConfig"`
}

// TableConfig contains options for a specific table
type TableConfig struct {

	// Absolute or relative base path to a ".go" file to write this struct to:
	// '/internal/modules/file.go'
	Path string `yaml:"path"`

	// Name of the Go package used for this file
	PackageName string `yaml:"packageName"`

	// Instead of only including the ID of a FK as a field, a full reference to the
	// struct is used for the speicified column names.
	// This is used for "1:1" relationships.
	// Specifiy a single element '*' to include all structs
	IncludeReferencedStructs []string `yaml:"includeReferencedStructs"`

	// Include additional fields for structs that references this table as an array.
	// This is used for "1:n" relationships.
	// To construct a "n:m" relationship you have to add a extra config for the zwischentabelle
	// that only specifies "IncludeRefrencedStructs" for the other column.
	// Note: you have to provide all referenced tables in "CreateStructs"
	IncludePointedStructs bool `yaml:"includePointedStructs"`

	// Sufix to add to the struct name. Add <empty> for no string and override of the default behaviour
	Suffix string `yaml:"suffix"`
}

type constructor struct {
	config *StructConfig
	tables []*ddl.Table
}

// CreateStructs creates all ".go" files with the structs based on the provided configuration
// and Tables.
// For any existing go files we have to patch, it's expected that it was created by this function
// or the file content was linted with golint
func CreateStructs(conf *StructConfig, tables []*ddl.Table) error {
	c := &constructor{
		config: conf,
		tables: tables,
	}

	// Loop through all tables
	for _, t := range c.tables {

		// Get table configuration to use
		tblConfig := c.getTableConfigForTable(t)

		// Read (existing) file
		fileExists := false
		if _, err := os.Stat(tblConfig.Path); err == nil {
			fileExists = true
		} else if errors.Is(err, os.ErrNotExist) {
			fileExists = false
		} else {
			return fmt.Errorf("failed to read properties of file %q: %s", tblConfig.Path, err)
		}

		// Read existing content
		content := ""
		if fileExists {
			cnt, err := os.ReadFile(tblConfig.Path)
			if err != nil {
				return fmt.Errorf("failed to read file %q: %s", tblConfig.Path, err)
			}
			content = string(cnt)
		}

		// Get new file content and writeto file
		newContent := c.getGoFile(content, t, tblConfig)
		f, err := os.Create(tblConfig.Path)
		if err != nil {
			return fmt.Errorf("failed to open file %q: %s", tblConfig.Path, err)
		}

		_, err = f.WriteString(newContent)
		if err != nil {
			f.Close()
			return fmt.Errorf("failed to write file %q: %s", tblConfig.Path, err)
		}
		f.Close()

		// Lint go file
		cmd := exec.Command("go", "fmt", tblConfig.Path)
		if err := cmd.Run(); err != nil {
			logger.Warning("Failed to run go fmt: %s", err)
		}
		cmd.Wait()
	}

	return nil
}

// GetGoFileName returns the name of a go file for a table
func GetGoFileName(tableName string) string {
	// Go files are all lowercased
	return strings.ToLower(tableName)
}

// GetFieldName returns the name of a struct or field from a database
// name
func GetFieldName(fieldName string) string {

	// Lowercase by default
	fieldName = strings.ToLower(fieldName)

	// Underscores are normally seperator.
	// We capitalize the first letter of every new "word"
	rtc := ""
	for _, w := range strings.Split(fieldName, "_") {
		if w != "" {
			rtc += strings.ToUpper(w[0:1]) + w[1:]
		}
	}

	return rtc
}

// GetJsonName returns the json key value for the provided fildName of
// the database.
// The json keys are CamelCased
func GetJsonName(fieldName string) string {
	structField := GetFieldName(fieldName)

	// Lowercase the first character for json
	r, size := utf8.DecodeRuneInString(structField)
	if r == utf8.RuneError && size <= 1 {
		return structField
	}
	lc := unicode.ToLower(r)
	if r == lc {
		return structField
	}
	return string(lc) + structField[size:]
}

// findTableConfig returns a specific table configuration for the table
// or an empty configuration struct if no one was provided
func (c *constructor) findTableConfig(tbl *ddl.Table) *TableConfig {

	// Schema + table has priority for table
	if val, ok := c.config.Tableconfig[tbl.Schema+"."+tbl.Name]; ok {
		return val
	}

	// By table name
	if val, ok := c.config.Tableconfig[tbl.Name]; ok {
		return val
	}

	return &TableConfig{}
}

// getTableConfigForTable returns a specific table configuration with defaults
// from the generic table configuration
func (c *constructor) getTableConfigForTable(tbl *ddl.Table) *TableConfig {
	tblConfig := c.findTableConfig(tbl)
	if tblConfig.Path == "" {
		// No specific path provided -> use from table name
		tblConfig.Path = c.config.GenericOutputPath + GetGoFileName(tbl.Name) + ".go"
	}
	if tblConfig.PackageName == "" {
		tblConfig.PackageName = c.config.PackgeName
	}
	if tblConfig.Suffix == "<empty>" {
		tblConfig.Suffix = ""
	} else if tblConfig.Suffix == "" {
		tblConfig.Suffix = c.config.Suffix
	}

	return tblConfig
}

// getGoFile returns the content of a go file for the specified table and configuration.
// If a existing go file exist, the struct will be updated with the new content
func (c *constructor) getGoFile(existingContent string, tbl *ddl.Table, tblConfig *TableConfig) string {
	rtc := ""
	columns := ""

	// Add struct type header
	tableName := GetFieldName(tbl.Name) + tblConfig.Suffix
	rtc += fmt.Sprintf("type %s struct {\n", tableName)
	columns += fmt.Sprintf("// %s\nconst (\n", tableName)

	// Add columns
	imports := make(map[string]bool, 0)
	for _, col := range tbl.Columns {

		// Add comments
		if col.Comment != "" {
			for _, comment := range strings.Split(col.Comment, "\n") {
				rtc += fmt.Sprintf("\t// %s\n", comment)
			}
		}

		// Initialize tags
		tags := GetColumnTag(col)

		// Get data type to use
		dataType, imp := c.getDataType(col, tblConfig, tags)
		if imp != "" {
			if _, exists := imports[imp]; !exists {
				imports[imp] = true
			}
		}

		fieldName := GetFieldName(col.Name)
		jsonName := GetJsonName(col.Name)
		rtc += fmt.Sprintf("\t%s %s `json:\"%s\" %s:\"%s\"`\n", fieldName, dataType, jsonName, ColumnTagId, tags.ToTag())

		// We also add the full reference to the column inside the string value.
		// It's needed to reference it without information of the table (which we can't get
		// with constants and no support for package reflection)
		identifier := tbl.Name + "." + col.Name
		if tbl.Schema != "" {
			identifier = tbl.Schema + "." + identifier
		}

		columns += fmt.Sprintf("\t %s_%s string = \"%s|%s\"\n", tableName, fieldName, fieldName, identifier)
	}

	// Add foreign key columns
	rtcAdd, columnsAdd, importsAdd := c.getOneToMany(tblConfig, tbl)
	if rtcAdd != "" {
		rtc += rtcAdd
		for _, imp := range importsAdd {
			if _, exists := imports[imp]; !exists {
				imports[imp] = true
			}
		}
	}
	columns += columnsAdd

	// Add metadata tag
	metaData := &MetadataTag{
		Schema: tbl.Schema,
		Table:  tbl.Name,
	}
	rtc += fmt.Sprintf("\t%s any `json:\"-\" %s:\"%s\"`\n", MetadataFieldName, MetadataTagId, metaData.ToTag())

	// Add closing line
	rtc += "}\n"
	columns += ")\n"

	// Add package header if no file exists already
	if existingContent == "" {
		header := fmt.Sprintf("package %s\n\n", tblConfig.PackageName)
		importStr := ""
		if len(imports) != 0 {
			importStr = "import (\n"
			for key := range imports {
				importStr += "\t\"" + key + "\"\n"
			}
			importStr += ")\n"
		}

		rtc = header + importStr + "\n" + rtc + columns
	} else {
		rtc = c.patchFile(existingContent, rtc+columns, tbl, tblConfig, imports)
	}

	return rtc
}

// getDataType returns the data type to use for the column as a string expression
// and the extra imports required for this data type.
// The tags my be updated within this function
func (c *constructor) getDataType(column *ddl.Column, tblConfig *TableConfig, _ *ColumnTag) (name string, imp string) {

	// Find 1:1 relationship
	if oneToOne := c.findOneToOne(column, tblConfig); oneToOne != "" {
		return oneToOne, ""
	}

	// Try to use sql null strings
	if column.CanBeNull {
		switch column.Type {
		case ddl.StringType:
			return "sql.NullString", "database/sql"
		case ddl.IntType:
			return "sql.NullInt64", "database/sql"
		case ddl.DoubleType:
			return "sql.NullFloat64", "database/sql"
		case ddl.DateType:
			return "sql.NullTime", "database/sql"
		}
	}

	switch column.Type {
	case ddl.StringType:
		return "string", ""
	case ddl.IntType:
		return "int", ""
	case ddl.DoubleType:
		return "float64", ""
	case ddl.DateType:
		return "time.Time", "time"
	}

	return "any", ""
}

// findOneToOne tries to find a 1:1 relationship by scanning the foreign keys of a column
// and the specified table configuration.
// It returns an empty string if no relationship was found or it's disable in the config
func (c *constructor) findOneToOne(column *ddl.Column, tblConfig *TableConfig) string {

	// Check if we have a foreign key for this column.
	// Otherwise we can't and don't reference another struct
	if !column.ForeignKey {
		return ""
	}

	// If the first element contains "*", we apply it for each table
	includeReference := len(tblConfig.IncludeReferencedStructs) == 1 && tblConfig.IncludeReferencedStructs[0] == "*"

	// Try to find by column name
	if !includeReference {
		for _, c := range tblConfig.IncludeReferencedStructs {
			if c == GetFieldName(column.Name) || c == column.Name {
				includeReference = true
			}
		}
	}

	// Nothing to do here
	if !includeReference {
		return ""
	}

	// Find the other table referenced by the foreign key
	for _, t := range c.tables {
		if t.Schema == column.ForeignKeyColumn.Schema && t.Name == column.ForeignKeyColumn.Name {
			// Get the table name
			tblConfRef := c.getTableConfigForTable(t)
			return "*" + GetFieldName(t.Name) + tblConfRef.Suffix
		}
	}

	logger.Debug("Found no foreign key reference for '%s.%s'", column.ForeignKeyColumn.Schema, column.ForeignKeyColumn.Name)

	return ""
}

// getOneToMany tries to find a 1:n relationship by scanning the foreign keys of all
// other tables to this table.
// It returns an empty string if no relationship was found or it's disable in the config.
// Otherwise this function returns any additional fields to add to the struct with it's required imports
func (c *constructor) getOneToMany(tblConfig *TableConfig, tbl *ddl.Table) (rtc string, constValues string, imp []string) {
	imports := []string{}

	// The user explicity has to enable this feature
	if !tblConfig.IncludePointedStructs {
		return rtc, constValues, imports
	}

	// Loop through every table and column and find any foreign key to this table
	for _, t := range c.tables {

		// Get the table configuration
		tblConfRef := c.getTableConfigForTable(t)

		// Loop through all columns to find a foreign key
		for _, c := range t.Columns {
			if c.ForeignKey && c.ForeignKeyColumn.Schema == tbl.Schema && c.ForeignKeyColumn.Name == tbl.Name {
				tblName := GetFieldName(t.Name) + tblConfRef.Suffix
				tag := &ColumnTag{
					PointedKeyReference: t.Schema + "." + t.Name + "." + c.Name,
				}
				rtc += fmt.Sprintf("\t%s []*%s `%s:\"%s\"`\n", GetFieldName(t.Name), tblName, ColumnTagId, tag.ToTag())

				// We also add the full reference to the column inside the string value.
				fieldNameRoot := GetFieldName(t.Name)
				identifier := tbl.Name + "." + fieldNameRoot
				if tbl.Schema != "" {
					identifier = tbl.Schema + "." + identifier
				}

				constValues += fmt.Sprintf("\t %s_%s string = \"%s|#%s\"\n", GetFieldName(tbl.Name)+tblConfig.Suffix, fieldNameRoot, fieldNameRoot, identifier)
			}
		}
	}

	// No relation found
	return rtc, constValues, imports
}

// patchFile patches the content of an existing file with the new struct.
// Any existing struct with that name will be overwritten
func (c *constructor) patchFile(existingContent string, newStruct string, tbl *ddl.Table, tblConfig *TableConfig, imports map[string]bool) (newContent string) {

	// Patch imports
	existingContent, err := c.patchImports(existingContent, imports)
	if err != nil {
		logger.Error("Failed to patch imports for table %s.%s: %s", tbl.Schema, tbl.Name, err)
	}

	// Find existing struct config
	tblName := GetFieldName(tbl.Name) + tblConfig.Suffix
	reg := regexp.MustCompile(
		fmt.Sprintf(
			`type %s struct {(.|\n)*?\s*%s.*\n}((//.*)|(\s|\n)*)*const \((.|\n)*?\)\n`,
			tblName, MetadataFieldName,
		),
	)

	if reg.MatchString(existingContent) {
		// Replace content
		return reg.ReplaceAllString(existingContent, newStruct)
	} else {
		// Append content
		return existingContent + "\n" + newStruct
	}
}

func (c *constructor) patchImports(existingContent string, imports map[string]bool) (string, error) {
	if len(imports) == 0 {
		return existingContent, nil
	}

	// Regex to find an import statement
	reg := regexp.MustCompile(`"([^"]+)"`)

	var importStart, importEnd int
	importFound := true
	// Find any existing import clause within the first 5 lines
	for i, line := range strings.Split(existingContent, "\n") {
		// No import found
		if i > 5 && importStart == 0 {
			importFound = false
		}

		// Trim any whitespace for import
		lineContent := strings.Trim(line, " \t\n")

		// Search for import mode
		if importStart == 0 {
			if strings.HasPrefix(lineContent, "import") {
				importStart = i
				// The opening bracket HAS to stand on the same line (when linted with go)
				if strings.Contains(lineContent, "(") {
					// We have to parse it further
					continue
				} else {
					// Extract imported package
					matches := reg.FindStringSubmatch(lineContent)
					if len(matches) >= 2 {
						// We can only import ONE package without ()
						if _, exists := imports[matches[1]]; !exists {
							imports[matches[1]] = true
						}
						importEnd = i
						break
					} else {
						return existingContent, fmt.Errorf("not a valid import statment: %s", lineContent)
					}
				}
			} else {
				// Nothing to do
				continue
			}
		}

		// Parse multiline ()
		if lineContent == ")" {
			importEnd = i
			break
		}

		matches := reg.FindStringSubmatch(lineContent)
		if len(matches) >= 2 {
			if _, exists := imports[matches[1]]; !exists {
				imports[matches[1]] = true
			}
		} else {
			return existingContent, fmt.Errorf("not a valid multiline import statment: %s", lineContent)
		}
	}

	// Build a new import string. We wan't to sort it
	keys := make([]string, 0)
	for k := range imports {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newImport := "import (\n"
	for _, k := range keys {
		newImport += fmt.Sprintf("\t"+`"%s"`+"\n", k)
	}
	newImport += ")"

	// We didn't found an existing import statment yet that we could replace
	if !importFound {
		lines := strings.Split(existingContent, "\n")

		// Only a single line -> add it directly below
		if len(lines) <= 1 {
			lines = append(lines, "")
			lines = append(lines, strings.Split(newImport, "\n")...)
		} else {
			// Insert it into existing, empty line
			return replaceLines(existingContent, 1, 1, strings.Split("\n"+newImport, "\n")), nil
		}

		return strings.Join(lines, "\n"), nil
	}

	// Replace import string
	return replaceLines(existingContent, importStart, importEnd, strings.Split(newImport, "\n")), nil
}

// replaceLines removes the lines identified by "startLine" and "endLine" and inserts the new lines
// at it's position.
// The line seperator has to be "\n"
func replaceLines(content string, startLine int, endLine int, newLines []string) string {
	lines := strings.Split(content, "\n")

	// Inset new lines by position
	lines = append(lines[:startLine], append(newLines, lines[endLine+1:]...)...)

	// Join strings together again
	return strings.Join(lines, "\n")
}
