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
bash run-regression-test.sh --run -d external_table_p0/mongodb -s test_mongodb_native
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
