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

// Regression for the time-travel row-count / CBO cardinality skew.
//
// WHY this matters: the shared cross-statement row-count cache is keyed by table only and computes at
// the LATEST snapshot. A FOR VERSION AS OF query's scan reads the PINNED snapshot, but without a
// snapshot dimension the optimizer estimated its cardinality from the latest count -> skewed join
// reorder / build-side selection (estimate-only; results stay correct). After the fix, a versioned read
// computes the row count AT the pinned snapshot, so the estimated cardinality matches the rows scanned.
//
// Data comes from create_preinstalled_scripts/paimon/run09.sql: table tbl_time_travel gets one 3-row
// INSERT per snapshot, so snapshot 1 has 3 rows while the latest table has 18. The explain cardinality
// of a FOR VERSION AS OF 1 scan must therefore be the pinned 3, not the latest 18.
suite("test_paimon_time_travel_rowcount", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_paimon_time_travel_rowcount_catalog"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """
    sql """switch `${catalog_name}`"""
    sql """use test_paimon_time_travel_db"""
    // Fetch the external row count synchronously so the latest-snapshot cardinality is populated
    // (the row-count cache is async by default and would otherwise report UNKNOWN on the first call).
    sql """set fetch_hive_row_count_sync=true"""

    // Extract the scan cardinality from an EXPLAIN plan.
    def cardinalityOf = { String query ->
        def plan = sql("explain ${query}").collect { it[0] }.join("\n")
        def matcher = (plan =~ /cardinality=(\d+)/)
        assertTrue(matcher.find(), "no cardinality found in explain for: ${query}\n${plan}")
        return matcher.group(1) as long
    }

    long ttCardinality = cardinalityOf("select * from tbl_time_travel FOR VERSION AS OF 1")
    long latestCardinality = cardinalityOf("select * from tbl_time_travel")
    logger.info("tbl_time_travel cardinality: time-travel(snapshot 1)=${ttCardinality}, latest=${latestCardinality}")

    // The time-travel scan must be estimated at the PINNED snapshot's row count (3), not the latest (18).
    assertEquals(3L, ttCardinality)
    assertEquals(18L, latestCardinality)
    // Guard the core invariant independent of the exact numbers: time-travel to an older, smaller snapshot
    // must estimate fewer rows than the latest table (before the fix both were the latest count).
    assertTrue(ttCardinality < latestCardinality,
            "time-travel cardinality ${ttCardinality} must be < latest ${latestCardinality}")

    sql """drop catalog if exists ${catalog_name}"""
}
