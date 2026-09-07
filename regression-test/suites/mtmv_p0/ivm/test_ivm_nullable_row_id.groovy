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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_nullable_row_id") {

    // =========================================================
    // Part 1: Single group-by key that contains a NULL group.
    // The single INT key widens losslessly to largeint, so the
    // group row-id is cast(k1 AS LARGEINT) and the NULL-key group
    // has a NULL row-id. Delta apply must match it null-safely.
    // =========================================================

    sql """drop materialized view if exists test_ivm_null_rowid_agg_mv;"""
    sql """drop table if exists test_ivm_null_rowid_agg_base;"""

    sql """
        CREATE TABLE test_ivm_null_rowid_agg_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Groups: k1=1, k1=2 and a NULL-key group
    sql """
        INSERT INTO test_ivm_null_rowid_agg_base VALUES
            (1, 10),
            (2, 20),
            (NULL, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_null_rowid_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM test_ivm_null_rowid_agg_base GROUP BY k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_null_rowid_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_null_rowid_agg_mv")

    // NULL-key group materializes as its own row (NULL row-id)
    order_qt_agg_null_group_initial """SELECT k1, cnt, sum_v1 FROM test_ivm_null_rowid_agg_mv"""

    // Update the NULL-key group via MOW upsert: (NULL, 30) -> (NULL, 99)
    sql """INSERT INTO test_ivm_null_rowid_agg_base VALUES (NULL, 99);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_null_rowid_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_null_rowid_agg_mv")

    order_qt_agg_null_group_after_update """SELECT k1, cnt, sum_v1 FROM test_ivm_null_rowid_agg_mv"""

    // Mix: delete the NULL-key group and add a new non-NULL group in one refresh
    sql """DELETE FROM test_ivm_null_rowid_agg_base WHERE k1 IS NULL;"""
    sql """INSERT INTO test_ivm_null_rowid_agg_base VALUES (3, 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_null_rowid_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_null_rowid_agg_mv")

    order_qt_agg_null_group_after_delete """SELECT k1, cnt, sum_v1 FROM test_ivm_null_rowid_agg_mv"""

    sql """drop materialized view if exists test_ivm_null_rowid_agg_mv;"""
    sql """drop table if exists test_ivm_null_rowid_agg_base;"""

    // =========================================================
    // Part 2: Simple linear MV over a single-key MOW table with
    // NULL key rows. The scan row-id is cast(k1 AS LARGEINT), so
    // the NULL-key row gets a NULL row-id in the MV unique key.
    // =========================================================

    sql """drop materialized view if exists test_ivm_null_rowid_linear_mv;"""
    sql """drop table if exists test_ivm_null_rowid_linear_base;"""

    sql """
        CREATE TABLE test_ivm_null_rowid_linear_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_null_rowid_linear_base VALUES
            (1, 10),
            (2, 20),
            (NULL, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_null_rowid_linear_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM test_ivm_null_rowid_linear_base WHERE v1 > 0;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_null_rowid_linear_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_null_rowid_linear_mv")

    // NULL-key row is carried into the MV
    order_qt_linear_null_key_initial """SELECT k1, v1 FROM test_ivm_null_rowid_linear_mv"""

    // Update the NULL-key row: (NULL, 30) -> (NULL, 99). The delta row has row-id NULL and
    // must replace the stored NULL row-id row (delete-bitmap keyed on the unique key).
    sql """INSERT INTO test_ivm_null_rowid_linear_base VALUES (NULL, 99);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_null_rowid_linear_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_null_rowid_linear_mv")

    order_qt_linear_null_key_after_update """SELECT k1, v1 FROM test_ivm_null_rowid_linear_mv"""

    // Delete the NULL-key row
    sql """DELETE FROM test_ivm_null_rowid_linear_base WHERE k1 IS NULL;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_null_rowid_linear_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_null_rowid_linear_mv")

    order_qt_linear_null_key_after_delete """SELECT k1, v1 FROM test_ivm_null_rowid_linear_mv"""

    sql """drop materialized view if exists test_ivm_null_rowid_linear_mv;"""
    sql """drop table if exists test_ivm_null_rowid_linear_base;"""
}
