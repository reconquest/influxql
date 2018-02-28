package influxql

import (
	"bytes"
)

// CreateDatabase represents a SHOW TAG KEYS statement.
type CreateDatabaseBuilder struct {
	database Builder
}

// CreateDatabase creates a SHOW query.
func CreateDatabase(name string) *CreateDatabaseBuilder {
	return &CreateDatabaseBuilder{
		database: &literal{name},
	}
}

// Build satisfies Builder.
func (s *CreateDatabaseBuilder) Build() (string, error) {
	data := createDatabaseTemplateValues{}

	if err := compileInto(s.database, &data.Database); err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(nil)
	err := createDatabaseTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
