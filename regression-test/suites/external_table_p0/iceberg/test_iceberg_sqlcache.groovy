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

// WHY: an iceberg table must be able to use SqlCache. The eligibility "quiet window" gate compares
// (now_millis - table_newest_update). iceberg reports its newest-update time in MICROSECONDS, and before the
// fix that raw micros value was fed into the gate, so (now - micros) was always <= 0 and never cleared the
// window -> iceberg (and any query joining it) was NEVER cacheable. The fix gives the gate a genuine
// wall-clock millis (connector-normalized), so a quiet iceberg table becomes cacheable; a write still
// invalidates via the version token (no stale results).
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
        sql """CREATE TABLE ${tbl} (id INT, v INT)"""
        sql """INSERT INTO ${tbl} VALUES (1, 10), (2, 20)"""

        sql """set enable_sql_cache=true"""

        def q = "select * from ${db}.${tbl} order by id".toString()

        // Quiet past the window since the last write, then create the cache entry. The gate must pass for the
        // entry to be stored — before the fix it never did for iceberg, so assertHasCache below stayed red.
        sleep(quietWaitMillis)
        sql "${q}"
        assertHasCache q

        // A write changes the table's version token, so the cached entry is invalidated and the plan must NOT
        // reuse it — proving the fix never serves stale results (the token guard is independent of the gate).
        sql """INSERT INTO ${tbl} VALUES (3, 30)"""
        assertNoCache q
    } finally {
        sql """drop table if exists ${catalog_name}.${db}.${tbl}"""
        sql """drop database if exists ${catalog_name}.${db} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}
