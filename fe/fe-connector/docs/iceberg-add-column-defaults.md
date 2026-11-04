# Adding Iceberg columns with defaults

On format-version 3 tables, `ALTER TABLE ... ADD COLUMN` can add a required
field when its default is a non-NULL value supported by the column type.
This applies to top-level fields, fields inside a STRUCT, and STRUCT fields
inside ARRAY elements or MAP values:

```sql
ALTER TABLE t ADD COLUMN added INT NOT NULL DEFAULT 7;
ALTER TABLE t ADD COLUMN (required_value INT NOT NULL DEFAULT 8, optional_value INT);
ALTER TABLE t ADD COLUMN s.added INT NOT NULL DEFAULT 9;
ALTER TABLE t ADD COLUMN arr.element.added INT NOT NULL DEFAULT 11;
ALTER TABLE t ADD COLUMN m.value.added INT NOT NULL DEFAULT 13;
```

Each added field gets the same typed value as its `initial-default` and
`write-default`. Existing data files are not rewritten: readers use the
initial default when the field ID is absent. Write statements use the
write schema bound for that statement, and schema changes after binding
are still rejected by the existing commit checks.

Boolean defaults accept `0`/`1` and `false`/`true`. DATE and DATETIME defaults
accept validated SQL date and timestamp strings, preserving fractional
seconds. The conversion also retains the existing numeric epoch forms
used by connector callers.

An optional parent STRUCT, ARRAY element, or MAP value can remain NULL even
when a child field is required. A child default does not create a non-NULL
parent. Explicit NULL values in supplied required fields are rejected rather
than replaced by defaults.

Format-version 1 and 2 tables do not accept non-NULL column defaults.
Required additions without a non-NULL default are rejected. MAP key schema
evolution, ON UPDATE, and authoring non-NULL complex defaults remain outside
this operation. A batch ADD commits all fields together; failure leaves the
previous schema intact.

Iceberg defaults remain distinct from generic Doris catalog column defaults.
Cached read columns do not acquire write defaults, so DESC and
information_schema.columns.COLUMN_DEFAULT keep their existing behavior.
SHOW CREATE TABLE preserves source requiredness.

Coverage: IcebergAddColumnDefaultsTest exercises metadata conversion and SDK
commits. test_iceberg_add_column_defaults covers the SQL path with Parquet and
ORC, old and new files, NULL parents, required-field failures and older formats.
