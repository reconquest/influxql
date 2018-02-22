package influxql

import (
	"bytes"
	"errors"
)

// ShowFieldKeys represents a SHOW FIELD KEYS statement.
type ShowFieldKeysBuilder struct {
	measurement Builder
	rp          Builder
}

// ShowFieldKeys creates a SHOW query.
func ShowFieldKeys() *ShowFieldKeysBuilder {
	return &ShowFieldKeysBuilder{}
}

// From represents the FROM in SHOW x FROM.
func (s *ShowFieldKeysBuilder) From(measurement string) *ShowFieldKeysBuilder {
	s.measurement = &literal{measurement}
	return s
}

// RetentionPolicy represents a retention policy part of FROM statement.
func (s *ShowFieldKeysBuilder) RetentionPolicy(rp string) *ShowFieldKeysBuilder {
	s.rp = &literal{rp}
	return s
}

// Build satisfies Builder.
func (s *ShowFieldKeysBuilder) Build() (string, error) {
	data := showFieldKeysTemplateValues{}

	if s.measurement != nil {
		if err := compileInto(s.measurement, &data.Measurement); err != nil {
			return "", err
		}
	}

	if s.rp != nil {
		if err := compileInto(s.rp, &data.RetentionPolicy); err != nil {
			return "", err
		}

		if s.measurement == nil {
			return "", errors.New(
				"retention policy specified, but measurement was not specified",
			)
		}
	}

	buf := bytes.NewBuffer(nil)
	err := showFieldKeysTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
