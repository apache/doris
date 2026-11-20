# MongoDB regression fixture

This optional standalone MongoDB 8.0.20 service supplies deterministic BSON data for the native MongoDB Catalog and the embedded Trino 435 MongoDB connector. Start it with:

```bash
CONTAINER_UID=doris-mongodb-test- bash docker/thirdparties/run-thirdparties-docker.sh -c mongodb
```

Set `MONGODB_PORT` to override the published port. Restarting this stack recreates the synthetic database `doris_mongodb_test`; do not point it at a production database. Both MongoDB data directories use disposable tmpfs mounts, so recreating or stopping the stack leaves no anonymous data volumes. The service authenticates with the fixture account `mongodb_test` / `mongodb_test` in `admin`. Readiness requires the final network-facing MongoDB process and the completed fixtures.

Install the complete MongoDB plugin ZIP (`io.trino:trino-mongodb:435:zip`, published on Maven Central), or its directory from the Trino 435 server distribution in the operator-controlled Trino plugin directory on every FE and BE. Use the same connector version on both sides; copying only `trino-mongodb-435.jar` omits its runtime dependencies and service-provider metadata. Configure `trino_connector_plugin_dir` before starting Doris, and restart the processes after changing plugins. These suites use the preinstalled plugin and do not download or replace the other Trino fixtures.

Add these values to `regression-test/conf/regression-conf-custom.groovy`, using the reachable Docker host and published port:

```groovy
enableMongoDBTest = true
externalEnvIp = "127.0.0.1"
mongodb_port = 27017
mongodb_user = "mongodb_test"
mongodb_password = "mongodb_test"
```

Run the suites with the normal runner:

```bash
bash run-regression-test.sh --run -d external_table_p0/trino_connector/mongodb
```

| Suite | Coverage |
|---|---|
| `test_trino_mongodb_metadata` | Discovery, explicit and inferred schemas, hidden `_id`, custom schema collections, refresh, missing tables and unsupported types |
| `test_trino_mongodb_types` | ObjectId and ordinary Binary, numerical/date/string values, unicode, null/missing fields, nullable nested rows, arrays, mixed numeric types and empty collections |
| `test_trino_mongodb_queries` | Scalar filters, null logic, residual predicates with Limit, projections, sort/offset, aggregates, joins, subqueries, CTEs, unions, windows, array expansion, views, joins with internal tables and hexadecimal materialization |
| `test_trino_mongodb_batches` | 2,081 documents across Mongo cursor and Trino page boundaries, exact byte conversion, aggregates and repeated early termination |
| `test_trino_mongodb_json` | Native JSON values and functions, BSON documents, JSON null versus SQL NULL, nested JSON in arrays/maps/rows, multi-page reads, views and internal-table materialization |

ObjectId maps to `VARBINARY(12)`, retaining the original bytes. Use `from_binary(_id)` for a deterministic hexadecimal string in filters, joins and grouping. Native scalar VARBINARY comparisons, grouping and join keys retain their existing Doris restrictions and are covered by negative tests. These suites do not claim arbitrary BSON-to-VARIANT support, CDC, writes, transactions, replica-set failover or parallel range splitting.

Most collections have explicit `_schema` records; `inferred_records` intentionally exercises automatic initialization. The connector may write to `_schema` even for read queries. The invalid schema fixture intentionally declares an unsupported interval column to verify error reporting. Metadata refreshes must account for both Doris and connector caches; refresh tests here re-read an unchanged schema and do not claim immediate invalidation of the connector cache after external changes.

## JSON columns

Fields explicitly declared as `json` in the connector's `_schema` collection map to native Doris `JSON`. This also applies to JSON fields inside Trino `array`, `map` and `row` types. JNI transfers the connector's UTF-8 JSON text and parses it into Doris JSONB; SQL NULL and the JSON value `null` remain distinct. Scalar JSON supports JSON functions and internal-table materialization. Nested JSON can be queried through the catalog; internal-table creation retains the engine's existing complex-type restrictions, including rejecting `ARRAY<JSON>`.

Casting a complex value containing JSON to `STRING` preserves each JSON value's own quotes. JSON children use lowercase `null` for JSON null and uppercase `NULL` for SQL NULL, so a cast back to the original array, map or struct type preserves the distinction. This typed SQL text can contain `NULL` and is therefore not always a standalone JSON document.

For example, the fixture declares `json_records.payload` as `json`. Its BSON document is queryable with:

```sql
SELECT seq, json_extract_string(payload, '$.city')
FROM mongodb_catalog.doris_mongodb_test.json_records
WHERE json_extract_int(payload, '$.n') = 7;
```

Automatic schema inference still maps BSON documents to Trino `row` / Doris `STRUCT`. Use an explicit `json` declaration when a field should be read as a whole JSON value. BSON documents use the connector's Extended JSON representation, so embedded ObjectIds appear as `{"$oid":"..."}` objects. For BSON string values declared as `json`, their contents must be valid JSON text; malformed text fails the query. The JSON suite covers both real BSON documents and valid JSON text, plus a malformed-text negative case.

## Native MongoDB Catalog

`02-native-fixtures.js` initializes the separate `doris_mongodb_native` database.
Its fixture contains validator-based collections, a computed view without `_id`,
a time-series collection with duplicate timestamps, BSON special values, malformed
historical values, and a reader granted `find` only on the view. These tests use
the native FE plugin and C++ reader and need no Trino plugin installation.

Use the same regression configuration above, then run:

```bash
bash run-regression-test.sh --run -d external_table_p0/mongodb
```

The suite covers auto/explicit/document schemas, exact ObjectId and int64 values,
Decimal128 scales, UTC dates, binary subtypes, nested arrays/structs, canonical
Extended JSON, missing/null/mismatch handling, SQL filters and LIKE/REGEXP,
aggregations, joins, windows, multi-batch reads, repeated early termination,
views, time-series measurements and view-only authorization. Legal tuple/pattern/
dependency validators verify complete document fallback. A 900,000-container BSON
fixture exercises scalar projection through whole-document validation; the separate
`doris_mongodb_pagination` database has 140 empty collections to cross the metadata
cursor's 128-name batch boundary. Binary displays and
analysis use `FROM_BINARY`; direct VARBINARY operations keep Doris's existing
analysis restrictions.

`test_mongodb_native_acceptance` also checks authentication failure and recovery
after credential correction, rejection of unsupported read-concern/range policies,
local SQL case/NULL semantics over a non-simple-collation view, and materialization
into an internal table with a two-way result difference. Batch-boundary queries
explicitly set `batch_size=1024`: the default batch size can hold the entire
2,081-row fixture without issuing a `getMore`.

### Native cursor fault checks

`verify_native_lifecycle.py` exercises real FE-to-BE scans with MongoDB command
failpoints. Run it separately against an otherwise idle, disposable copy of this
fixture started with `mongod --setParameter enableTestCommands=1`. It reserves the
`mongodb_native_lifecycle` catalog, uses the initialized `doris_mongodb_native`
data, and requires a MongoDB account allowed to configure failpoints and read
server status. The supplied URI and Doris credentials come from the environment.

Install `pymongo` and `pymysql` in a test virtual environment, then run:

```bash
export MONGODB_URI='mongodb://<fixture-user>:<fixture-password>@<fixture-host>:<port>/?authSource=admin'
export DORIS_FE_HOST='<fe-host>'
export DORIS_FE_PORT='<mysql-port>'
export DORIS_FE_USER='<test-user>'
export DORIS_FE_PASSWORD='<test-password>'
python3 docker/thirdparties/docker-compose/mongodb/verify_native_lifecycle.py --allow-failpoints
```

The checks cover EOF, repeated LIMIT, decoding errors, an injected `getMore`
command error, socket/query timeouts, cancellation after a blocked `getMore` has
actually started, and concurrent reads. Every scenario checks source-side cursor
closure and a subsequent successful read. Failpoints target this test's application
name and are disabled in `finally` blocks. The catalog remains available for
inspection. This is a focused lifecycle check; the release soak and topology/TLS
matrices remain separate qualification work.

`03-m1-fixtures.js` creates the independent `doris_mongodb_m1` database for
`test_mongodb_m1`. The suite compares full sorted results with predicate/LIKE/projection
switches disabled and enabled, and covers dirty scalar/array values, exact int64 and
Decimal128 literals, UTC date ranges, nested rollback, subtype mismatches, source
collation, strict errors, residuals, views, time-series scans and statement overrides.
Its completed Profile check also verifies the selective candidate reduces `MongoRowsRead`
from ten source documents to one.

## Authenticated common-snapshot matrix

`snapshot_fixture.py` creates an independent Linux host-network fixture with a
three-member replica set, two single-member shards, a configuration replica set,
and two mongos routers. It pins `mongo:7.0.28` or `mongo:8.0.20`, enables test commands,
and uses keyfile authentication. Data is disposable tmpfs; credentials and the
recorded container names stay in the caller's private state directory. This is a
functional snapshot fixture; its single-member shards do not qualify shard failover.
Use an otherwise idle host and reachable advertised address. The fixture reserves
eight ports after `--base-port`; every process advertises the supplied host.

Install `pymongo` and `pymysql` in a test virtual environment. Supply synthetic
administrator credentials through the environment, then create and validate a fixture:

```bash
export MONGODB_SNAPSHOT_ADMIN_USER='<fixture-administrator>'
export MONGODB_SNAPSHOT_ADMIN_PASSWORD='<fixture-password>'
python3 docker/thirdparties/docker-compose/mongodb/snapshot_fixture.py \
  --state-dir '<private-state-directory>' --image mongo:8.0.20 \
  --base-port 28010 --advertised-host '<reachable-fixture-host>'
export DORIS_FE_HOST='<fe-host>'
export DORIS_FE_PORT='<mysql-port>'
export DORIS_FE_HTTP_ADDRESS='<fe-host>:<http-port>'
export DORIS_FE_USER='<doris-test-user>'
export DORIS_FE_PASSWORD='<doris-test-password>'
python3 docker/thirdparties/docker-compose/mongodb/verify_native_snapshot.py \
  --fixture-state '<private-state-directory>/state.json' --allow-failpoints --iterations 20
```

The verifier recreates only `doris_mongodb_snapshot` in this disposable fixture. It
creates read-only query users with data/metadata access plus the narrow snapshot
identity privilege (`replSetGetConfig`, or `find` on `config.version`). Administrators
are used only for fixture control and source-oracle checks. Two 2,081-row collections
cross source/Doris batch boundaries; an atomic transaction changes both bank balances
while SQL reads. Each native scan's Profile must report the same T, and the complete
aggregate tuples must match independent source reads at that exact T. Cross-router
catalogs exercise one cluster identity. Precision cases retain a 34-significant-digit
Decimal128, pre-epoch milliseconds, DATETIME(3/6), and four session time zones.

Command failpoints cover getMore failure, SnapshotTooOld propagation, observed
getMore cancellation/deadline, concurrent transaction visibility, replica-set
configuration changes, DROP/CREATE replacement, and cleanup/recovery. A one-client-per-BE
pool with unequal query deadlines verifies that remaining-time differences cannot
create independent pools. Time-series cases cover logical duplicate timestamps,
concurrent inserts and observed public-options changes; concurrent drop/rename is
explicitly outside the qualified time-series source contract. Failpoints are always
disabled in `finally` blocks. The test does not enable range scans or claim a release
soak. Repeat using `--image mongo:7.0.28` and a separate state directory/port range.

For the checked-in SQL regression, seed a fresh fixture with the same verifier's
`--prepare-only` option, then set these private regression settings. Read the query
URI from `reader_uris[0]` in its mode-0600 state file; do not commit that URI.

```groovy
enableMongoSnapshotTest = true
mongodb_snapshot_uri = '<authenticated-replica-set-query-uri>'
```

```bash
bash run-regression-test.sh --run -d external_table_p0/mongodb -s test_mongodb_snapshot
python3 docker/thirdparties/docker-compose/mongodb/snapshot_fixture.py \
  --state-dir '<private-state-directory>' --remove
```

`--remove` removes only containers recorded by that fixture. Preserve its state
privately until cleanup, since it contains credentials. Stop/removal discards data;
no fixture data volumes are created. The standard standalone suites continue to use
the separate original fixture and remain required alongside this snapshot matrix.

Run the complete fault matrix with one BE so both competing queries exercise the
same per-BE pool. To additionally qualify a cluster with at least two BEs, rerun
the verifier with `--oracle-only --require-cross-backend --iterations 20`. This mode
requires a query in each deployment's concurrent oracle to execute actual MongoDB
scanners on at least two distinct BEs, as recorded next to the scanner timestamps
in completed Profiles. Aggregate-only fragments do not count as native scans.
The oracle enables the existing all-candidate external-scan spreading policy through
statement hints, since default single-split placement may choose one BE for both
collections. Every scan still has to share T and match the source result at T. It deliberately
omits the per-BE pool-contention fault case in this multi-BE run.
