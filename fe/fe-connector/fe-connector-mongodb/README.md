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
UTC BSON Date ranges (including time-series time fields), positive IN, BETWEEN, scalar IS NOT NULL,
and safe AND/OR combinations. Numeric comparisons keep their BSON type and exact precision;
integer mappings without source tags, DOUBLE/NaN, NOT/!=/NOT IN, IS NULL under mismatch-to-NULL,
functions, CAST/TRY_CAST, and nested/array paths remain local. An OR requires a safe candidate
for every arm. LIKE sends an escaped required literal substring as a conservative regex;
wildcard-only, ambiguous escapes, NUL and REGEXP stay local. Doris always evaluates the full
LIKE pattern, including anchors, wildcards, Unicode and newline rules.

Join, aggregation, sort and LIMIT remain Doris operations. Range splitting and LIMIT pushdown
remain separate work. Common snapshots are described below. Use matching FE/BE versions; the new optional BSON
filter/projection fields preserve the single-cursor descriptor layout.

`batch_size` and `query_timeout` are session variables. The following session variables
also support statement-level `SET_VAR` hints; values are case-insensitive:

| Session variable | Default | Supported values |
|---|---|---|
| `mongodb_read_preference` | `inherit` | `inherit`, `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`, `nearest` |
| `mongodb_read_concern` | `inherit` | `inherit`, `local`, `majority`, `available`, `snapshot` |

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
secondary in a replica set. Ordinary read concerns control visibility independently of node selection.
Only the explicit `snapshot` policy establishes a common point across MongoDB scans.

Catalog connection defaults:

| Property | Default |
|---|---|
| `mongodb.read_preference` | `primary` |
| `mongodb.read_concern` | `majority` (`local`, `available`, and `snapshot` also supported) |
| `mongodb.connect_timeout_ms` | `10000` |
| `mongodb.server_selection_timeout_ms` | `10000` |
| `mongodb.socket_timeout_ms` | `1000` |
| `mongodb.pool_max_clients` | `16` |
| `mongodb.datetime_timezone` | `UTC` |

A majority read does not imply a point-in-time snapshot. User-supplied time-travel
timestamps and range scans remain unsupported. EOF, early termination, cancellation and errors close the
cursor and return its exclusive client lease. Pool identities include catalog
configuration and credentials. Driver errors expose stable error codes without
source values or connection strings. Keep network timeouts small enough for the
required cancellation responsiveness.

## Common snapshots

Set `mongodb_read_concern='snapshot'` for a session or a `SET_VAR` statement, or
`mongodb.read_concern='snapshot'` for a catalog. Matching FE/BE versions are required.
The qualified server versions are MongoDB 7.0 and 8.0 (wire versions 21 and 25),
on replica sets and mongos. Standalone deployments and standard views are rejected
at planning time. Ordinary majority/local/available reads keep their existing behavior.

```sql
SELECT /*+ SET_VAR(mongodb_read_concern='snapshot') */ l.total + r.total
FROM (SELECT sum(amount) total FROM mongo_native.sample.left_records) l
CROSS JOIN (SELECT sum(amount) total FROM mongo_native.sample.right_records) r;
```

The FE runs a bounded one-row snapshot probe, explicitly closes its server-bound
cursor, and takes **only** the server's `cursor.atClusterTime`. One statement owns one timestamp T, shared across
its qualified MongoDB scans, including catalogs that refer to the same cluster.
Mixing snapshot and ordinary MongoDB reads, or independent source clusters, fails.
The native reader sends a typed BSON Timestamp in `readConcern.atClusterTime`,
using a separate non-causal session and a server-bound cursor for each stream.
It retains that server/session for getMore and killCursors; sessions and cursors
are never shared across BE threads. The native command path sends the explicit
find command because the pinned C driver's ordinary find helper drops the timestamp.
EXPLAIN reports the policy/T, and Profile reports `MongoReadConcern` and
`MongoSnapshotTimestamp` for each scanner.

Snapshot readers need full collection metadata in addition to data `find` privileges.
A replica-set reader also needs the cluster action `replSetGetConfig`; a mongos reader
needs `find` on `config.version` to identify the deployment consistently across routers.
These metadata privileges are specific to snapshot qualification; normal reads do not
require the new identity probes. Source identity and replica-set configuration version
are pinned and checked before reading and at cursor EOF. Collection UUIDs are checked
as well. An observed replacement, configuration change, permission error, history
expiration (`SnapshotTooOld`), command error, or deadline fails the statement. There
is no automatic timestamp reselection, retry at a newer T, or downgrade to majority.
A new statement may select a new T after refreshing changed namespace metadata.
History retention must cover planning and all source reads; source socket and query
timeouts still bound cancellation responsiveness.

Time-series reads use the public logical namespace and pin its public options;
MongoDB 7.0/8.0 do not expose a UUID for that namespace. The qualified combination is
snapshot reads with concurrent inserts and **without concurrent drop/rename** of the
time-series collection. An observed options change fails the read. Options equality
cannot detect dropping and recreating an identical time-series definition, so that
DDL combination is outside this contract. No internal bucket reads are introduced.
Snapshot consistency does not imply parallel range partitioning; each namespace
continues to use one native cursor.

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

The snapshot unit tests cover statement isolation, shared timestamp selection,
concurrent planners, sticky failures, source identity/configuration, BSON timestamp
bounds, UUID byte order, unsupported objects and deadline/cancellation admission.
`test_mongodb_snapshot` adds SQL differential, exact decimal/date/timezone, routing,
policy-restoration and completed-Profile checks. The separate authenticated fixture
and `verify_native_snapshot.py` exercise real MongoDB 7/8 replica sets and two routers,
transactional writes with a source-at-T oracle, getMore failures, cancellation,
namespace/configuration changes, time-series inserts/options, and client-pool bounds.
See the fixture README for reproducible commands and the explicit source restrictions.
