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

suite("test_ivm_refresh_dry_run") {
    // Cloud mode: __DORIS_SEQUENCE_COL__ in the dry-run delta rows derives from cloud txn
    // versioning and differs from local (e.g. 6145 vs 4097), so the .out values
    // for the sequence column do not apply.
    if (isCloudMode()) {
        logger.info("skip test_ivm_refresh_dry_run on cloud mode: " +
                "__DORIS_SEQUENCE_COL__ differs between cloud and local")
        return
    }
    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_refresh_dry_run_mv"
    sql "DROP TABLE IF EXISTS test_ivm_refresh_dry_run_base"

    sql """
        CREATE TABLE test_ivm_refresh_dry_run_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """
        INSERT INTO test_ivm_refresh_dry_run_base VALUES
            (1, 10), (2, 20), (3, 30)
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_refresh_dry_run_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM test_ivm_refresh_dry_run_base
           GROUP BY k1
    """

    sql "REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_refresh_dry_run_mv")

    sql "INSERT INTO test_ivm_refresh_dry_run_base VALUES (1, 15), (4, 40)"

    order_qt_ivm_dry_run_before "SELECT k1, cnt, sum_v1 FROM test_ivm_refresh_dry_run_mv"

    order_qt_ivm_dry_run_full """
        REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL WITH DRY RUN
    """

    // The delta rows picked by a LIMIT depend on scan order (no ORDER BY before the cap),
    // so assert only the returned row count instead of exact rows in the .out file.
    assertEquals(1, sql("REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL WITH DRY RUN LIMIT 1").size())
    assertEquals(1, sql("REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL WITH DRY RUN LIMIT 1, 1").size())
    assertEquals(0, sql("REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL WITH DRY RUN LIMIT 0").size())

    order_qt_ivm_dry_run_after "SELECT k1, cnt, sum_v1 FROM test_ivm_refresh_dry_run_mv"

    order_qt_ivm_dry_run_repeat """
        REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL WITH DRY RUN LIMIT 10
    """

    sql "REFRESH MATERIALIZED VIEW test_ivm_refresh_dry_run_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_refresh_dry_run_mv")
    order_qt_ivm_dry_run_after_refresh "SELECT k1, cnt, sum_v1 FROM test_ivm_refresh_dry_run_mv"
}
