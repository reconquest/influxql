package influxql

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"
)

const placeholder = "?"

type nullValue struct{}

var (
	reWhiteChars        = regexp.MustCompile(`[\s\t\r\n]+`)
	reSpacesBetweenTags = regexp.MustCompile(`}}[\s\t\r\n]+{{`)
)

var (
	orKeyword  = &keyword{"OR"}
	andKeyword = &keyword{"AND"}
)

func cleanTemplate(s string) string {
	s = reWhiteChars.ReplaceAllString(s, " ")
	s = reSpacesBetweenTags.ReplaceAllString(s, "}}{{")
	return strings.TrimSpace(s)
}

const showFieldKeysTemplateText = `
	SHOW FIELD KEYS
	{{- if or .Measurement .RetentionPolicy}} FROM {{end}}
	{{- with .RetentionPolicy}}{{.}}.{{end}}
	{{- with .Measurement }}{{.}}{{end}}
`

type showFieldKeysTemplateValues struct {
	Measurement     string
	RetentionPolicy string
}

const showTagKeysTemplateText = `
	SHOW TAG KEYS
	{{- if or .Measurement .RetentionPolicy}} FROM {{end}}
	{{- with .RetentionPolicy}}{{.}}.{{end}}
	{{- with .Measurement }}{{.}}{{end}}
`

type showTagKeysTemplateValues struct {
	Measurement     string
	RetentionPolicy string
}

const createDatabaseTemplateText = `
	CREATE DATABASE {{.Database}}
`

type createDatabaseTemplateValues struct {
	Database string
}

const showMeasurementsTemplateText = `
	SHOW MEASUREMENTS
`

type showMeasurementsTemplateValues struct{}

const showRetentionPoliciesTemplateText = `
	SHOW RETENTION POLICIES
`

type showRetentionPoliciesTemplateValues struct{}

const createRetentionPolicyTemplateText = `
	{{if .IsAlter}}ALTER{{else}}CREATE{{end}}
		RETENTION POLICY {{.Name}} ON {{.Database}}
		DURATION {{.Duration}}
		REPLICATION {{.Replication}}
		{{with .ShardDuration}} SHARD DURATION {{.}}{{end}}
		{{if .IsDefault}} DEFAULT{{end}}
`

type createRetentionPolicyTemplateValues struct {
	IsAlter       bool
	Name          string
	Database      string
	Duration      string
	Replication   int
	ShardDuration string
	IsDefault     bool
}

const selectTemplateText = `
	SELECT
		{{with .Fields}}
			{{joinWithCommas .}}
		{{else}}
			*
		{{end}}
	FROM
		{{with .RetentionPolicy}}{{.}}.{{end}}{{.Measurement}}
	{{with .Where}}
		WHERE
		 {{joinWithSpace .}}
	{{end}}
	{{with .GroupBy}}
		GROUP BY
		 {{joinWithCommas .}}
	{{end}}
	{{with .OrderBy}}
		ORDER BY
		 {{joinWithCommas .}}
	{{end}}
	{{with .Limit}} LIMIT {{.}}{{end}}
	{{with .Offset}} OFFSET {{.}}{{end}}
	{{with .SLimit}} SLIMIT {{.}}{{end}}
	{{with .SOffset}} SOFFSET {{.}}{{end}}
	{{with .Fill}} fill({{.}}){{end}}
`

type selectTemplateValues struct {
	Measurement     string
	RetentionPolicy string
	Fields          []string
	Where           []string
	GroupBy         []string
	OrderBy         []string
	Fill            string
	Limit           int
	Offset          int
	SLimit          int
	SOffset         int
}

const deleteTemplateText = `
	DELETE FROM
		{{.Measurement}}
	{{with .Where}}
		WHERE
		 {{joinWithSpace .}}
	{{end}}
`

type deleteTemplateValues struct {
	Measurement string
	Where       []string
}

func joinWithCommas(in []string) string {
	return strings.Join(in, ", ")
}

func joinWithSpace(in []string) string {
	return strings.Join(in, " ")
}

var templateFuncs = map[string]interface{}{
	"joinWithCommas": joinWithCommas,
	"joinWithSpace":  joinWithSpace,
}

var selectTemplate = template.Must(
	template.New("select").Funcs(templateFuncs).
		Parse(cleanTemplate(selectTemplateText)),
)

var deleteTemplate = template.Must(
	template.New("delete").Funcs(templateFuncs).
		Parse(cleanTemplate(deleteTemplateText)),
)

var createRetentionPolicyTemplate = template.Must(
	template.New("createRetentionPolicy").Funcs(templateFuncs).
		Parse(cleanTemplate(createRetentionPolicyTemplateText)),
)

var showFieldKeysTemplate = template.Must(
	template.New("showFieldKeys").Funcs(templateFuncs).
		Parse(cleanTemplate(showFieldKeysTemplateText)),
)

var showTagKeysTemplate = template.Must(
	template.New("showTagKeys").Funcs(templateFuncs).
		Parse(cleanTemplate(showTagKeysTemplateText)),
)

var showMeasurementsTemplate = template.Must(
	template.New("showMeasurements").Funcs(templateFuncs).
		Parse(cleanTemplate(showMeasurementsTemplateText)),
)

var createDatabaseTemplate = template.Must(
	template.New("createDatabase").Funcs(templateFuncs).
		Parse(cleanTemplate(createDatabaseTemplateText)),
)

var showRetentionPoliciesTemplate = template.Must(
	template.New("showRetentionPolicies").Funcs(templateFuncs).
		Parse(cleanTemplate(showRetentionPoliciesTemplateText)),
)

type keyword struct {
	v string
}

func (k *keyword) Build() (string, error) {
	return k.v, nil
}

type order struct {
	field literal
	order string
}

func (order *order) Build() (string, error) {
	field, err := order.field.Build()
	if err != nil {
		return "", err
	}

	return field + " " + strings.ToUpper(order.order), nil
}

// Expr represents an expression.
type Expr struct {
	expr   string
	values []interface{}
}

// Build satisfies Builder.
func (e *Expr) Build() (string, error) {
	placeholders := strings.Count(e.expr, placeholder)

	if placeholders > 0 {
		// Where("foo = ?", "bar")
		if placeholders != len(e.values) {
			return "", fmt.Errorf(
				"Mismatched number of placeholders (%d) and values (%d)",
				strings.Count(e.expr, placeholder),
				len(e.values),
			)
		}
	} else {
		if len(e.values) > 0 {
			parts := strings.Split(strings.TrimSpace(reWhiteChars.ReplaceAllString(e.expr, " ")), " ")
			lparts := len(parts)

			if lparts < 1 {
				return "", fmt.Errorf("Expecting statement.")
			} else if lparts < 2 {
				// Where("foo", "bar")
				if len(e.values) != 1 {
					return "", fmt.Errorf("Expecting exactly one value.")
				}
				e.expr = fmt.Sprintf("%q = ?", parts[0])
			} else if lparts < 3 {
				// Where("foo =", "bar")
				if len(e.values) != 1 {
					return "", fmt.Errorf("Expecting exactly one value.")
				}
				e.expr = fmt.Sprintf("%q %s ?", parts[0], parts[1])
			} else {
				return "", fmt.Errorf("Unsupported expression %q", e.expr)
			}
		}
	}

	compiled := make([]interface{}, 0, len(e.values))
	for i := range e.values {
		lit := &value{e.values[i]}
		c, err := lit.Build()
		if err != nil {
			return "", err
		}
		compiled = append(compiled, c)
	}

	s := strings.Replace(e.expr, "?", "%s", -1)
	return fmt.Sprintf(s, compiled...), nil
}

type value struct {
	v interface{}
}

func (v *value) Build() (string, error) {
	switch t := v.v.(type) {
	case string:
		return fmt.Sprintf(`'%s'`, t), nil
	case int, uint, int64, uint64, int32, uint32, int8, uint8:
		return fmt.Sprintf("%d", t), nil
	case time.Time:
		return fmt.Sprintf(`'%s'`, t.Format("2006-01-02T15:04:05Z")), nil
	case time.Duration:
		return timeFormat(t), nil
	default:
		return fmt.Sprintf(`'%v'`, t), nil
	}
	panic("reached")
}

type literal struct {
	v interface{}
}

func (l *literal) Build() (string, error) {
	switch v := l.v.(type) {
	case Builder:
		return v.Build()
	case time.Duration:
		t := Time(v)
		return t.Build()
	case string:
		if strings.ContainsAny(v, `".`) {
			return fmt.Sprintf(`%s`, v), nil
		}

		if v == "*" {
			return v, nil
		}

		return fmt.Sprintf(`%q`, v), nil
	default:
		return fmt.Sprintf(`"%v"`, v), nil
	}
	panic("reached")
}

func compileInto(src Builder, dst *string) (err error) {
	*dst, err = src.Build()
	return
}

func compileArrayInto(src []Builder, dst *[]string) error {
	v := make([]string, 0, len(src))
	for i := range src {
		s, err := src[i].Build()
		if err != nil {
			return err
		}
		v = append(v, s)
	}
	*dst = v
	return nil
}
