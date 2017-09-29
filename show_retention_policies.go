package influxql

import (
	"bytes"
)

// ShowRetentionPolicies represents a SHOW MEASUREMENTS statement.
type ShowRetentionPoliciesBuilder struct {
}

// ShowRetentionPolicies creates a SHOW query.
func ShowRetentionPolicies() *ShowRetentionPoliciesBuilder {
	return &ShowRetentionPoliciesBuilder{}
}

// Build satisfies Builder.
func (s *ShowRetentionPoliciesBuilder) Build() (string, error) {
	data := showRetentionPoliciesTemplateValues{}

	buf := bytes.NewBuffer(nil)
	err := showRetentionPoliciesTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
