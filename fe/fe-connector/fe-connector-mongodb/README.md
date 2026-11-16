# Native MongoDB Catalog

The FE plugin uses MongoDB's synchronous Java driver for metadata. BE reads logical
collections directly with `libmongoc` and `libbson`; BSON values are converted by
Doris `DataTypeSerDe` implementations. No BE JVM or Trino connector is required.
The plugin archive is deployed to `plugins/connector/mongodb/` by `build.sh --fe`.

```sql
CREATE CATALOG mongo_native PROPERTIES (
    "type" = "mongodb",
    "mongodb.uri" = "mongodb://mongo.example:27017",
    "mongodb.username" = "reader",
    "mongodb.password" = "example-password",
    "mongodb.auth_database" = "admin",
    "mongodb.databases" = "sample",
    "mongodb.schema.mode" = "auto"
);
```

The database allowlist is optional. It also permits discovery with accounts that
cannot list all databases. Reads require `find` on the logical collection or view.
Metadata discovery uses authorized, name-only listing. Validator schemas require
full `listCollections` metadata; explicit/document schemas also work with restricted
view-only readers. The pinned Java driver core operation returns a server-bound
metadata cursor with both authorization flags and object kinds; its public names
iterable exposes only strings. The driver owns continuation and cursor cleanup.
Views are queried by their own names, preserving their pipeline
and collation. Time-series collections return logical measurements, including
measurements without a unique `_id`; `system.buckets` is never exposed or read.

## Schema policies

- `auto` (default): explicit mapping for the namespace, otherwise an ordinary
  collection's fully representable `$jsonSchema` validator, otherwise document mode.
  It never samples rows or silently drops unrepresentable declared fields.
- `validator`: require a representable validator on an ordinary collection.
- `explicit`: require a catalog-owned mapping for each queried namespace.
- `document`: `_mongo_ejson` contains canonical Extended JSON and `_mongo_bson`
  contains the original BSON bytes. Extended JSON preserves BSON type tags; it is
  not ordinary JSON with automatic Doris numeric coercion.

Validators constrain writes, not all historical documents. Mapped columns and
nested fields remain nullable. Tuple-array items, schema combinations, pattern-based
properties and schema-valued dependencies/additional properties require an explicit
mapping or whole-collection document fallback, including inside nested objects.
Missing fields and BSON null both become SQL NULL;
the original BSON preserves the distinction. Unsupported combinations use document
mode or produce an explicit error. Refresh metadata after changing a validator,
explicit mapping, view, or collection definition:

```sql
REFRESH CATALOG mongo_native;
```

An explicit schema is a JSON object in `mongodb.schema`, keyed by `database.collection`:

```json
{
  "sample.records": {
    "columns": [
      {"name":"identifier","path":["_id"],"bson_types":["objectId"],"doris_type":"VARBINARY(12)"},
      {"name":"amount","path":["amount"],"bson_types":["decimal"],"doris_type":"DECIMAL(20,2)"},
      {"name":"literal","path":["a.b"],"doris_type":"STRING"},
      {"name":"nested","path":["a","b"],"doris_type":"STRING"},
      {"name":"payload","path":["payload"],"bson_types":["binData"],"subtype":0,"doris_type":"VARBINARY"}
    ]
  }
}
```

Paths contain exact case-sensitive segments. Doris column/struct names are folded
to lowercase. An optional `children` array overrides struct-field or array-element
bindings using the same shape. Struct fields are matched by name; arrays require
one element binding. Free-form objects and heterogeneous arrays can map to JSON.

ObjectId maps to its raw 12 bytes: use `FROM_BINARY(identifier)` for hex display
and supported string comparisons, joins, or grouping. Direct VARBINARY operators
retain Doris's existing analysis restrictions.
The declared VARBINARY length is schema metadata; decoding preserves the full bytes,
matching the existing VARBINARY readers.
BinData mapped to VARBINARY requires an explicit subtype; arbitrary subtypes can be
preserved in Extended JSON. Timestamp maps to
`STRUCT<seconds:BIGINT,increment:BIGINT>`, preserving both unsigned 32-bit components.
BSON date maps to UTC `DATETIME(3)` or explicit BIGINT epoch milliseconds. Decimal128
requires an explicit DECIMAL precision/scale for typed conversion; validator-derived
decimals use Extended JSON because they have no common fixed scale.

`mongodb.type_mismatch` defaults to `error`. Choosing `null` replaces a failed top-level
field with SQL NULL after rolling back nested data; it does not suppress transport
failures or replace a malformed array element while keeping a partial array. Numeric
narrowing, decimal rounding, datetime precision loss, incompatible BSON tags/subtypes,
and values outside the target range are rejected. No implicit string coercion occurs.

## Execution policy

This implementation emits one split per logical namespace. The native cursor handles
find/getMore batches. Safe source projection includes complete top-level roots required by
output columns and every residual predicate; whole-document bindings and literal dot/dollar
roots keep full documents. Whole roots preserve scalar-ancestor errors for nested mappings.

`enable_mongodb_predicate_pushdown`, `enable_mongodb_like_pushdown`, and
`enable_mongodb_projection_pushdown` default to `true` and support `SET` and `SET_VAR`.
The predicate switch gates LIKE as well. Every predicate remains a Doris residual, and
EXPLAIN reports `SUPERSET` or `UNSUPPORTED` without source values or credentials.

Predicate candidates are enabled only with explicit `mongodb.type_mismatch=null`.
The default `error` policy keeps filters local: filtering on one valid field could otherwise
hide a conversion failure in another projected field, and validators do not prove historical
data is clean. `enable_strict_cast=true` also keeps candidates local to preserve SQL
conversion errors. Standard views keep SQL predicates local and retain their own collation.
Collections and time-series scans with candidates use simple collation.

Supported candidates include scalar-root integer/boolean/string/Decimal128 comparisons,
UTC BSON Date ranges (including time-series time fields), positive IN, BETWEEN, IS NOT NULL,
and safe AND/OR combinations. Numeric comparisons keep their BSON type and exact precision;
integer mappings without source tags, DOUBLE/NaN, NOT/!=/NOT IN, IS NULL under mismatch-to-NULL,
functions, CAST/TRY_CAST, and nested/array paths remain local. An OR requires a safe candidate
for every arm. LIKE sends an escaped required literal substring as a conservative regex;
wildcard-only, ambiguous escapes, NUL and REGEXP stay local. Doris always evaluates the full
LIKE pattern, including anchors, wildcards, Unicode and newline rules.

Join, aggregation, sort and LIMIT remain Doris operations. Range splitting, LIMIT pushdown
and shared snapshots are separate work. Use matching FE/BE versions; the new optional BSON
filter/projection fields preserve the single-cursor descriptor layout.

`batch_size` and `query_timeout` are session variables. The following session variables
also support statement-level `SET_VAR` hints; values are case-insensitive:

| Session variable | Default | Supported values |
|---|---|---|
| `mongodb_read_preference` | `inherit` | `inherit`, `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`, `nearest` |
| `mongodb_read_concern` | `inherit` | `inherit`, `local`, `majority`, `available` |

`inherit` uses the catalog's effective policy: URI `readPreference` / `readConcernLevel`
first, then `mongodb.read_preference` / `mongodb.read_concern`. An explicit session or
statement value overrides that policy for data reads. Each scan pins its effective
values without changing the catalog, metadata discovery, or other sessions.
URI routing criteria (ordered tag sets, maximum staleness and hedge options) remain
in effect when overriding the mode. A `primary` override with incompatible URI
criteria fails at planning instead of silently dropping those criteria.

```sql
SET mongodb_read_preference = 'secondaryPreferred';
SET mongodb_read_concern = 'local';
SELECT /*+ SET_VAR(mongodb_read_preference='primary', mongodb_read_concern='majority') */
    count(*) FROM mongo_native.sample.records;
SET mongodb_read_preference = 'inherit';
SET mongodb_read_concern = 'inherit';
```

Selecting a secondary can return stale data. `primaryPreferred` and
`secondaryPreferred` permit fallback to the other role; `secondary` requires a
secondary in a replica set. A read concern controls read visibility independently
of node selection and does not establish a snapshot across Doris scans.

Catalog connection defaults:

| Property | Default |
|---|---|
| `mongodb.read_preference` | `primary` |
| `mongodb.read_concern` | `majority` (`local` and `available` also supported) |
| `mongodb.connect_timeout_ms` | `10000` |
| `mongodb.server_selection_timeout_ms` | `10000` |
| `mongodb.socket_timeout_ms` | `1000` |
| `mongodb.pool_max_clients` | `16` |
| `mongodb.datetime_timezone` | `UTC` |

A majority read does not imply a point-in-time snapshot. Explicit snapshot/time-travel
requests are unsupported. EOF, early termination, cancellation and errors close the
cursor and return its exclusive client lease. Pool identities include catalog
configuration and credentials. Driver errors expose stable error codes without
source values or connection strings. Keep network timeouts small enough for the
required cancellation responsiveness.

## Dependencies and tests

Java dependencies use a module-local MongoDB BOM; the FE host supplies SPI and logging.
Build the matching native libraries with:

```sh
bash thirdparty/build-thirdparty.sh mongo_c_driver
```

FE unit tests reside in this module, BSON SerDe unit tests in
`be/test/core/data_type_serde/data_type_serde_bson_test.cpp`, and reader lifecycle tests
in `be/test/format_v2/table/mongodb_reader_test.cpp`. The regression suites
`external_table_p0/mongodb/test_mongodb_native.groovy` and
`external_table_p0/mongodb/test_mongodb_read_policy.groovy` use the disposable MongoDB
fixture under `docker/thirdparties/docker-compose/mongodb/`, enabled with
`enableMongoDBTest`, `externalEnvIp`, `mongodb_port`, `mongodb_user`, and
`mongodb_password` regression settings.

The M1 suite `test_mongodb_m1` uses `03-m1-fixtures.js` and compares predicate,
LIKE and projection switches on/off over large integers, decimals, UTC dates,
missing/null/malformed values, arrays, nested structs, binary subtypes, collations,
views and time-series data. It also verifies strict error behavior, CAST/OR residuals,
LIMIT, statement-switch restoration and EXPLAIN. A completed Profile check verifies
`MongoRowsRead` changes from ten source documents to one for a selective candidate.
First-release typed mappings support
DECIMAL precision up to 38 and DATETIME scale 0..6 with no rounding; MAP, VARIANT,
implicit numeric/string conversions and other unlisted target types are unsupported.
Use JSON canonical Extended JSON or raw BSON to retain unsupported BSON tags.
