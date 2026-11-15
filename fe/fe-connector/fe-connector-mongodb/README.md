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
find/getMore batches. Projection is materialized locally, and every SQL filter,
LIKE/REGEXP, join, aggregation, sort and LIMIT remains a Doris operation. Range
splitting, predicate/LIMIT pushdown and shared snapshots are separate work.

`batch_size` and `query_timeout` are session variables. URI read preference settings
take precedence over the catalog mode, retaining ordered tag sets and maximum staleness
in both drivers. Without URI routing settings, `mongodb.read_preference` supplies the mode.
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
in `be/test/format_v2/table/mongodb_reader_test.cpp`. The regression suite
`external_table_p0/mongodb/test_mongodb_native.groovy` uses the disposable MongoDB
fixture under `docker/thirdparties/docker-compose/mongodb/`, enabled with
`enableMongoDBTest`, `externalEnvIp`, `mongodb_port`, `mongodb_user`, and
`mongodb_password` regression settings.
