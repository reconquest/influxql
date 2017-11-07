package influxql

import (
	"bytes"
	"errors"
)

// ShowTagKeys represents a SHOW TAG KEYS statement.
type ShowTagKeysBuilder struct {
	measurement Builder
	rp          Builder
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

// RetentionPolicy represents a retention policy part of FROM statement.
func (s *ShowTagKeysBuilder) RetentionPolicy(rp string) *ShowTagKeysBuilder {
	s.rp = &literal{rp}
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
	err := showTagKeysTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
