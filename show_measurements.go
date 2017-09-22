package influxql

import (
	"bytes"
)

// ShowMeasurements represents a SHOW MEASUREMENTS statement.
type ShowMeasurementsBuilder struct {
}

// ShowMeasurements creates a SHOW query.
func ShowMeasurements() *ShowMeasurementsBuilder {
	return &ShowMeasurementsBuilder{}
}

// Build satisfies Builder.
func (s *ShowMeasurementsBuilder) Build() (string, error) {
	data := showMeasurementsTemplateValues{}

	buf := bytes.NewBuffer(nil)
	err := showMeasurementsTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
