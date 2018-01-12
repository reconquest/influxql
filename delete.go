package influxql

import (
	"bytes"
)

// DeleteBuilder represents a DELETE statement.
type DeleteBuilder struct {
	measurement Builder
	retention   Builder
	where       []Builder
}

// Delete creates a DELETE query.
func Delete() *DeleteBuilder {
	builder := &DeleteBuilder{}
	return builder
}

// From represents the FROM in DELETE x FROM.
func (builder *DeleteBuilder) From(measurement string) *DeleteBuilder {
	builder.measurement = &literal{measurement}
	return builder
}

// Where replaces the current conditions.
func (builder *DeleteBuilder) Where(expr string, values ...interface{}) *DeleteBuilder {
	builder.where = make([]Builder, 0, 1)
	builder.where = append(builder.where, &Expr{expr: expr, values: values})
	return builder
}

// And adds a conjunction to the list of conditions.
func (builder *DeleteBuilder) And(expr string, values ...interface{}) *DeleteBuilder {
	if len(builder.where) > 0 {
		builder.where = append(builder.where, andKeyword, &Expr{expr: expr, values: values})
	} else {
		builder.where = append(builder.where, &Expr{expr: expr, values: values})
	}
	return builder
}

// Or adds a disjunction to the list of conditions.
func (builder *DeleteBuilder) Or(expr string, values ...interface{}) *DeleteBuilder {
	builder.where = append(builder.where, orKeyword, &Expr{expr: expr, values: values})
	return builder
}

// Build satisfies Builder.
func (builder *DeleteBuilder) Build() (string, error) {
	data := deleteTemplateValues{}

	if err := compileInto(builder.measurement, &data.Measurement); err != nil {
		return "", err
	}

	if err := compileArrayInto(builder.where, &data.Where); err != nil {
		return "", err
	}

	var err error

	buf := bytes.NewBuffer(nil)
	err = deleteTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
