package influxql

import (
	"bytes"
	"time"
)

// CreateRetentionPolicyBuilder represents a CREATE|IsAlter RETENTION POLICY statement.
type CreateRetentionPolicyBuilder struct {
	name        Builder
	database    Builder
	duration    Builder
	replication int
	shard       Builder
	isDefault   bool
	isAlter     bool
}

// CreateRetentionPolicy creates a CREATE|IsAlter RETENTION POLICY query.
func CreateRetentionPolicy(
	name string,
	database string,
	duration time.Duration,
	replication int,
) *CreateRetentionPolicyBuilder {
	return &CreateRetentionPolicyBuilder{
		name:        &literal{name},
		database:    &literal{database},
		duration:    &value{duration},
		replication: replication,
	}
}

// Shard represents SHARD DURATION "x"
func (s *CreateRetentionPolicyBuilder) ShardDuration(
	duration time.Duration,
) *CreateRetentionPolicyBuilder {
	s.shard = &value{duration}

	return s
}

// Alter changes CREATE query into ALTER query
func (s *CreateRetentionPolicyBuilder) Alter() *CreateRetentionPolicyBuilder {
	s.isAlter = true

	return s
}

// Default adds DEFAULT
func (s *CreateRetentionPolicyBuilder) Default() *CreateRetentionPolicyBuilder {
	s.isDefault = true

	return s
}

// Build satisfies Builder
func (s *CreateRetentionPolicyBuilder) Build() (string, error) {
	data := createRetentionPolicyTemplateValues{}

	if err := compileInto(s.name, &data.Name); err != nil {
		return "", err
	}

	if err := compileInto(s.database, &data.Database); err != nil {
		return "", err
	}

	if err := compileInto(s.duration, &data.Duration); err != nil {
		return "", err
	}
	if s.shard != nil {
		if err := compileInto(s.shard, &data.ShardDuration); err != nil {
			return "", err
		}
	}

	data.IsAlter = s.isAlter
	data.IsDefault = s.isDefault
	data.Replication = s.replication

	buf := bytes.NewBuffer(nil)
	err := createRetentionPolicyTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
