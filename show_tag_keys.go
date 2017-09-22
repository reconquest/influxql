package influxql

import (
	"bytes"
)

// ShowTagKeys represents a SHOW TAG KEYS statement.
type ShowTagKeysBuilder struct {
	measurement Builder
}

// ShowTagKeys creates a SHOW query.
func ShowTagKeys() *ShowTagKeysBuilder {
	return &ShowTagKeysBuilder{}
}

// From represents the FROM in SHOW x FROM.
func (s *ShowTagKeysBuilder) From(measurement string) *ShowTagKeysBuilder {
	s.measurement = &literal{measurement}
	return s
}

// Build satisfies Builder.
func (s *ShowTagKeysBuilder) Build() (string, error) {
	data := showTagKeysTemplateValues{}

	if s.measurement != nil {
		if err := compileInto(s.measurement, &data.Measurement); err != nil {
			return "", err
		}
	}

	buf := bytes.NewBuffer(nil)
	err := showTagKeysTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
