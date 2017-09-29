package influxql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testSamples = []struct {
	b Builder
	s string
	e bool
}{
	{
		Select("foo").From("bar"),
		`SELECT "foo" FROM "bar"`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location = ?`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE location = 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location != ?`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE location != 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location != 'Toronto'`),
		`SELECT "foo" FROM "bar" WHERE location != 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`time >`, time.Date(2015, 8, 18, 0, 0, 0, 0, time.UTC)),
		`SELECT "foo" FROM "bar" WHERE "time" > '2015-08-18T00:00:00Z'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`time > ?`, time.Date(2015, 8, 18, 0, 0, 0, 0, time.UTC)),
		`SELECT "foo" FROM "bar" WHERE time > '2015-08-18T00:00:00Z'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`"location" = ?`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`"location" = ? ?`, "Toronto"),
		``,
		true, // Unmatched ? placeholder.
	},
	{
		Select("foo").From("bar").Where(`"location" = ? AND "altitude" >= ?`, "Toronto", 500),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto' AND "altitude" >= 500`,
		false,
	},
	{
		Select("foo").From("bar").Where("location", "Toronto").And("altitude >=", 500),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto' AND "altitude" >= 500`,
		false,
	},
	{
		Select("foo").From("bar").Where("location", "Toronto").Or("altitude >=", 500),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto' OR "altitude" >= 500`,
		false,
	},
	{
		Select("foo").From("bar").Where("location", "Toronto").And("altitude >= x", 500),
		``,
		true, // Unsupported expression "altitude >= x"
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest"),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest'`,
		false, // Query is invalid, though.
	},
	{
		Select(Sum("value")).From("cpu").Where("region", "uswest"),
		`SELECT SUM("value") FROM "cpu" WHERE "region" = 'uswest'`,
		false, // Query is invalid, though.
	},
	{
		Select(Distinct("level description")).From("h2o_feet"),
		`SELECT DISTINCT("level description") FROM "h2o_feet"`,
		false,
	},
	{
		Select(Min("water_level"), Max("water_level")).From("h2o_feet"),
		`SELECT MIN("water_level"), MAX("water_level") FROM "h2o_feet"`,
		false,
	},
	{
		Select(Mean("water_level").As("dream_name")).From("h2o_feet"),
		`SELECT MEAN("water_level") AS "dream_name" FROM "h2o_feet"`,
		false,
	},
	{
		Select(Min("water_level").As("mwl"), Max("water_level").As("Mwl")).From("h2o_feet"),
		`SELECT MIN("water_level") AS "mwl", MAX("water_level") AS "Mwl" FROM "h2o_feet"`,
		false,
	},
	{
		Select(Count(Distinct("level description"))).From("h2o_feet"),
		`SELECT COUNT(DISTINCT("level description")) FROM "h2o_feet"`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Minute * 10)),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Minute * 10)).Fill(0),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(0)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Minute * 10)).Fill(nil),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(null)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Hour * 4)).Fill("none"),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(4h) fill(none)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").And("region", "uswest").GroupBy(Time(time.Hour * 4)).Fill("none"),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(4h) fill(none)`,
		false,
	},
	{
		Select("*").From("bar"),
		`SELECT * FROM "bar"`,
		false,
	},
	{
		Select("foo").From("bar").RetentionPolicy("week"),
		`SELECT "foo" FROM "week"."bar"`,
		false,
	},
	{
		Select("foo").From("bar").RetentionPolicy("default"),
		`SELECT "foo" FROM "default"."bar"`,
		false,
	},
	{
		Select("*").From("bar").Limit(1),
		`SELECT * FROM "bar" LIMIT 1`,
		false,
	},
	{
		Select("*").From("bar").Limit(1).Offset(2),
		`SELECT * FROM "bar" LIMIT 1 OFFSET 2`,
		false,
	},
	{
		Select("*").From("bar").SLimit(1),
		`SELECT * FROM "bar" SLIMIT 1`,
		false,
	},
	{
		Select("*").From("bar").SLimit(1).SOffset(2),
		`SELECT * FROM "bar" SLIMIT 1 SOFFSET 2`,
		false,
	},
	{
		Select("*").From("bar").GroupBy("time"),
		`SELECT * FROM "bar" GROUP BY "time"`,
		false,
	},
	{
		Select("*").From("bar").OrderBy("time"),
		`SELECT * FROM "bar" ORDER BY "time"`,
		false,
	},
	{
		Select("*").From("bar").OrderBy(Desc("time")),
		`SELECT * FROM "bar" ORDER BY "time" DESC`,
		false,
	},
	{
		ShowTagKeys(),
		`SHOW TAG KEYS`,
		false,
	},
	{
		ShowTagKeys().From("bar"),
		`SHOW TAG KEYS FROM "bar"`,
		false,
	},
	{
		ShowMeasurements(),
		`SHOW MEASUREMENTS`,
		false,
	},
	{
		ShowRetentionPolicies(),
		`SHOW RETENTION POLICIES`,
		false,
	},
	{
		CreateRetentionPolicy("name", "db", time.Hour, 1),
		`CREATE RETENTION POLICY "name" ON "db" DURATION 1h REPLICATION 1`,
		false,
	},
	{
		CreateRetentionPolicy("name", "db", time.Hour, 1).Alter(),
		`ALTER RETENTION POLICY "name" ON "db" DURATION 1h REPLICATION 1`,
		false,
	},
	{
		CreateRetentionPolicy("name", "db", time.Hour, 1).ShardDuration(time.Minute),
		`CREATE RETENTION POLICY "name" ON "db" DURATION 1h REPLICATION 1 SHARD DURATION 1m`,
		false,
	},
	{
		CreateRetentionPolicy("name", "db", time.Hour, 1).Default(),
		`CREATE RETENTION POLICY "name" ON "db" DURATION 1h REPLICATION 1 DEFAULT`,
		false,
	},
}

func TestSelect(t *testing.T) {
	for _, sample := range testSamples {
		s, err := sample.b.Build()

		if sample.e {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, sample.s, s)
	}
}
