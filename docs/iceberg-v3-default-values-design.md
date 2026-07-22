# Iceberg V3 Default Values in Doris

## Background

Iceberg V3 defines two different default-value contracts:

- `initial-default` is a read-time value. A reader uses it when a selected data file was written
  before a field was added and therefore does not contain that field ID.
- `write-default` is a write-time value. A writer uses it when a known field is omitted or when a
  statement explicitly requests `DEFAULT`.

Adding a field with a default initially records the same value in both properties. A later default
update changes only `write-default`. Doris must therefore keep the two concepts separate instead
of representing both as one generic column default.

The feature is split into three reviewable pull requests. The pull requests are intentionally
ordered read first, write second, and metadata authoring last.

## Spark-Iceberg reference model

The executable reference is Spark 4.0.0 with Iceberg 1.10.1, matching the repository Docker
fixture. Iceberg keeps defaults as typed values in `NestedField`; its Spark Parquet reader converts
the typed `initialDefault` to Spark's internal representation when a projected field is absent.
The Iceberg schema-update API also preserves the distinction between adding a field with its
initial default and later updating only its write default.

Iceberg 1.10.1's ORC projection still rejects a physically missing field whose `initialDefault` is
non-NULL. That is an upstream implementation gap, not the V3 contract Doris should reproduce.
Doris therefore uses Spark-Iceberg as the reference for typed default conversion and implements the
same field-ID/default semantics independently in both its Parquet and ORC scan paths.

Doris follows that typed-constant model, with an explicit FE/BE transport boundary:

1. FE reads `NestedField.initialDefault()` from the query-bound Iceberg schema.
2. FE serializes primitive non-binary values through Iceberg's type-aware identity transform and
   complex values through Iceberg `SingleValueParser` JSON. UUID, FIXED, and BINARY use Base64 as
   direct defaults because a Thrift string cannot safely carry arbitrary bytes; recursive metadata
   also retains their Iceberg type identity for binary values inside complex JSON.
3. Each scanner has its own recursive parser and constructs an owning typed constant before
   materializing rows. V1 and V2 do not delegate missing-column materialization to each other.

This is intentionally different from feeding metadata text to the SQL parser. In particular,
strings containing quotes, binary zero bytes, decimals, dates, and timestamps retain their typed
value rather than being reinterpreted as SQL syntax.

## PR roadmap

### PR1: read `initial-default`

PR1 adds read support for every Iceberg type already mapped by Doris. It does not add any new
Iceberg-to-Doris type mapping.

The reader selects defaults from the schema bound to the query:

| Query | Schema used for `initial-default` |
| --- | --- |
| Ordinary current-table read | `table.schema()` |
| Explicit snapshot/time travel | Schema recorded by the selected snapshot |
| Explicit branch/tag/ref | Schema resolved for that reference |

Using `currentSnapshot.schemaId()` for an ordinary read is incorrect because a schema-only commit
can update `table.schema()` without creating a snapshot.

FE transports default metadata recursively by Iceberg field ID through the existing external
schema Thrift structure. The metadata includes the serialized value, a lossless Base64 marker for
UUID/FIXED/BINARY, and the Iceberg optional/required flag. Nested defaults do not depend on
`Column.defaultValue`. PR1 deliberately leaves that generic Doris field unset for Iceberg schema
columns so existing INSERT/MERGE code cannot mistake `initial-default` for `write-default` before
PR2 adds a schema-pinned write-default path.

File Scanner V1 and File Scanner V2 consume the same metadata contract but implement missing-field
materialization independently:

- V1 applies defaults in its Parquet and ORC schema-change readers.
- V2 applies defaults through `ColumnDefinition`, `TableColumnMapper`, and `TableReader`.

Both implementations follow the same rules:

1. A physically present field is read as stored, including an explicit NULL.
2. A physically absent field with `initial-default` is materialized as a typed constant.
3. A physically absent optional field without a default is NULL.
4. A physically absent required field without a default is an error.
5. A NULL parent struct, list element, or map value remains NULL. A nested default is applied only
   inside a present parent value.
6. A non-NULL struct default is the Iceberg V3 empty-object sentinel `{}`; its effective value is
   built recursively from the struct fields' own `initial-default` metadata. List and map defaults
   use Iceberg's single-value JSON (`[...]` and `{"keys": [...], "values": [...]}`), including nested
   primitive and complex values.

Complex-column coverage includes defaults on newly added children of structs, list-element
structs, and map-value structs, plus recursive decoding of a whole complex-field default. Iceberg
Java 1.10.1's public `Types.NestedField`/`UpdateSchema` path rejects every non-NULL nested-type
default, even though the V3 format specification permits the empty struct sentinel. The Docker
fixture therefore uses only metadata the official API can commit: it covers child defaults under
physical complex parents and wholly missing optional complex parents with NULL parent defaults.
Focused V1 and V2 unit tests cover the spec-level `{}`/list/map JSON decoder. The fixture never
hand-edits table metadata to bypass Iceberg validation.

Spark-Iceberg Docker generates the fixtures. It writes Parquet and ORC files using the old schema,
then uses the Iceberg Java API to add defaulted fields. The fixture changes `write-default` after
the add so an old file returning `initial-default` proves that the read path did not accidentally
consume the current write default. Regression tests force all four paths:

- File Scanner V1 with Parquet
- File Scanner V1 with ORC
- File Scanner V2 with Parquet
- File Scanner V2 with ORC

The type matrix is exactly the set already supported before PR1: BOOLEAN, INTEGER, LONG, FLOAT,
DOUBLE, DECIMAL, DATE, TIMESTAMP with and without zone, STRING, UUID, FIXED, BINARY, and their
existing ARRAY/MAP/STRUCT compositions. TIME, TIMESTAMP_NANO, VARIANT, and any other currently
unsupported mapping remain out of scope.

PR1 verifies predicates for fields absent from old files in forced-path regression tests. Focused
V1 and V2 BE tests verify equality-delete keys, including a key that is retained only in schema
history after being dropped. Iceberg 1.10.1 cannot plan that dropped-key sequence end to end; its
planner fix is upstream Iceberg #15268, so PR1 does not broaden scope with an Iceberg dependency
upgrade. Binary-like defaults are checked byte-for-byte, and regression output is generated only by
the Doris regression test runner.

### PR2: consume `write-default`

PR2 adds write-time consumption without changing Iceberg default metadata. It introduces a
statement-scoped, field-ID keyed context pinned to the target schema ID and format version.

The following write paths consume the current `write-default`:

- omitted columns in INSERT
- explicit `DEFAULT`
- reordered and multi-row VALUES
- MERGE `NOT MATCHED INSERT`
- `DEFAULT(column)` where Doris syntax supports it

Explicit NULL and explicit values are never replaced. UPDATE and MERGE UPDATE preserve their
existing semantics. Analyzer projection and the schema sent to the Iceberg sink must use the same
schema ID; schema skew fails before data is dispatched.

PR2 does not add CREATE/ALTER syntax and does not author or evolve default metadata.

### PR3: author and evolve default metadata

PR3 adds DDL support after the read and write execution paths are stable:

- CREATE COLUMN DEFAULT records the appropriate initial write default for a new table schema.
- ADD COLUMN DEFAULT records equal `initial-default` and `write-default` values.
- Adding a required field succeeds only with a valid non-NULL initial default.
- `ALTER COLUMN ... SET DEFAULT` changes only `write-default`.
- `ALTER COLUMN ... DROP DEFAULT` removes only `write-default`.

DDL validates the Iceberg format version, converts Doris constant expressions to typed Iceberg
literals, and exposes current write defaults through user-visible schema output. Nested ALTER
syntax is not expanded beyond syntax Doris already supports when PR3 is implemented.

## PR1 acceptance criteria

1. Every Iceberg type mapped by Doris before PR1 has an `initial-default` read test, and the mapping
   switch itself is unchanged.
2. Struct children, list-element struct children, and map-value struct children receive defaults by
   field ID in both scanners; focused tests also cover `{}` struct, list, and map default decoding.
3. Parent NULL values and explicitly stored child NULL values remain NULL.
4. UUID, FIXED, and BINARY defaults match their exact bytes in both mapping modes.
5. Ordinary reads observe schema-only evolution, while explicit historical reads use their bound
   historical schema.
6. Parquet and ORC fixtures are generated through Spark-Iceberg Docker without hand-edited Iceberg
   metadata.
7. V1 and V2 each pass independent unit tests and forced-path regression tests.
8. Regression predicates and focused V1/V2 equality-delete tests agree with the value materialized
   for an absent field; dropped equality-key metadata is recovered from schema history.
9. PR1 contains no write-default consumption, DDL implementation, writer change, dependency
   upgrade, or new type mapping.

## Review gate

After implementation, review the complete three-dot diff against the current Apache Doris master
using the method in `.github/workflows/code-review-runner.yml`:

1. Build a changed-file and risk summary plus a deduplicated issue ledger.
2. Run independent code-reviewer and architect passes.
3. Verify each candidate finding against the actual diff and fresh test evidence, fix substantiated
   findings, and repeat until the review converges.
4. The change is review-clear only when the code reviewer returns `APPROVE`, the architect returns
   `CLEAR`, and no verified blocking finding remains.

## Upstream references

- [Iceberg specification: default values and column projection](https://iceberg.apache.org/spec/#default-values)
- [Iceberg `SingleValueParser` typed JSON encoding](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SingleValueParser.java)
- [Spark 4.0 Parquet reader missing-field/default handling](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/SparkParquetReaders.java)
- [Iceberg 1.10.1 ORC schema projection](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/orc/src/main/java/org/apache/iceberg/orc/ORCSchemaUtil.java)
- [Iceberg schema-update default APIs](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SchemaUpdate.java)
