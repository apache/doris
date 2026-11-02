# MongoDB regression fixture

This optional standalone MongoDB 8.0.20 service supplies deterministic BSON data for the embedded Trino 435 MongoDB connector. It does not require a Trino server. Start it with:

```bash
CONTAINER_UID=doris-mongodb-test- bash docker/thirdparties/run-thirdparties-docker.sh -c mongodb
```

Set `MONGODB_PORT` to override the published port. Restarting this stack recreates the synthetic database `doris_mongodb_test`; do not point it at a production database. The image initializes its own disposable data volume and authenticates with the fixture account `mongodb_test` / `mongodb_test` in `admin`.

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

ObjectId maps to `VARBINARY(12)`, retaining the original bytes. Use `from_binary(_id)` for a deterministic hexadecimal string in filters, joins and grouping. Native scalar VARBINARY comparisons, grouping and join keys retain their existing Doris restrictions and are covered by negative tests. These suites do not claim arbitrary BSON-to-VARIANT support, CDC, writes, transactions, replica-set failover or parallel range splitting.

Most collections have explicit `_schema` records; `inferred_records` intentionally exercises automatic initialization. The connector may write to `_schema` even for read queries. The invalid schema fixture intentionally declares an unsupported `json` column to verify error reporting. Metadata refreshes must account for both Doris and connector caches; refresh tests here re-read an unchanged schema and do not claim immediate invalidation of the connector cache after external changes.
