// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// WHY: a (time-partitioned) iceberg table must be able to use SqlCache. Two independent gates control external
// SqlCache eligibility, so the test must satisfy both:
//   1) It is opt-in via `enable_hive_sql_cache` (default false) — set below. Without it BindRelation marks
//      every external lakehouse table unsupported and nothing is ever cached.
//   2) The table must be a valid MTMV-related table (exactly one year/month/day/hour partition transform) so
//      the connector reports a real data-version token. An UNPARTITIONED iceberg table reports token 0 and is
//      never cacheable (the version<=0 fail-safe in SqlCacheContext.addUsedTable) — hence the DAY(ts) spec.
// The bug this actually exercises: the eligibility "quiet window" gate compares (now_millis - table_newest_update).
// A partitioned iceberg table reports its newest-update time in MICROSECONDS, and before the fix that raw micros
// value was fed into the gate, so (now - micros) was always <= 0 and never cleared the window -> a partitioned
// iceberg table (and any query joining it) was NEVER cacheable. The fix gives the gate a genuine wall-clock
// millis (connector-normalized), so a quiet iceberg table becomes cacheable; a write still invalidates via the
// version token (no stale results).
suite("test_iceberg_sqlcache", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_sqlcache"
    String db = "test_iceberg_sqlcache_db"
    String tbl = "t_sqlcache"

    // Adapt to the env's quiet-window length instead of mutating the global FE config (which would race with
    // other cache suites): wait just past cache_last_version_interval_second since the last write.
    long intervalSec = 30
    def cfg = sql """admin show frontend config like 'cache_last_version_interval_second'"""
    if (cfg != null && cfg.size() > 0 && cfg[0].size() > 1) {
        intervalSec = cfg[0][1].toString().toLong()
    }
    long quietWaitMillis = (intervalSec + 3) * 1000L

    def assertHasCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            contains("PhysicalSqlCache")
        }
    }
    def assertNoCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            notContains("PhysicalSqlCache")
        }
    }

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    try {
        sql """switch ${catalog_name}"""
        sql """drop database if exists ${db} force"""
        sql """create database ${db}"""
        sql """use ${db}"""
        // A time-transform partitioned table (DAY(ts)) is a valid MTMV-related table, so the connector reports a
        // real (micros) newest-update token > 0 — the precondition for both eligibility gates. An unpartitioned
        // table would report token 0 and never cache (see the WHY note above).
        sql """CREATE TABLE ${tbl} (id INT, v INT, ts DATETIME) PARTITION BY LIST (DAY(ts)) ()"""
        sql """INSERT INTO ${tbl} VALUES (1, 10, '2024-01-01 08:00:00'), (2, 20, '2024-01-02 09:00:00')"""

        sql """set enable_sql_cache=true"""
        // External lakehouse SqlCache is opt-in (default off); without this the iceberg table is treated as an
        // unsupported table and the query is never cached.
        sql """set enable_hive_sql_cache=true"""

        def q = "select * from ${db}.${tbl} order by id".toString()

        // Quiet past the window since the last write, then create the cache entry. The gate must pass for the
        // entry to be stored — before the fix it never did for iceberg, so assertHasCache below stayed red.
        logger.info("Sleeping ${quietWaitMillis} ms (cache_last_version_interval_second=${intervalSec}s + 3s) "
                + "to pass the SqlCache quiet window since the last write, then build the cache entry")
        sleep(quietWaitMillis)
        sql "${q}"
        assertHasCache q

        // A write changes the table's version token, so the cached entry is invalidated and the plan must NOT
        // reuse it — proving the fix never serves stale results (the token guard is independent of the gate).
        sql """INSERT INTO ${tbl} VALUES (3, 30, '2024-01-03 10:00:00')"""
        assertNoCache q
    } finally {
        sql """drop table if exists ${catalog_name}.${db}.${tbl}"""
        sql """drop database if exists ${catalog_name}.${db} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}
