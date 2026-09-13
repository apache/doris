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

suite("test_ivm_varchar_key_rowid") {

    // A single key that cannot be losslessly widened to largeint (VARCHAR) must still be
    // used directly as the row-id (no hash), so the row-id column keeps the key type.
    // The DESC outputs below (with show_hidden_columns = true) capture the row-id column
    // type in the .out file for verification.

    // =========================================================
    // Part 1: Non-agg (linear) MV over a single VARCHAR-key MOW
    // table. The scan row-id is the VARCHAR key itself, so the
    // MV row-id hidden column must be VARCHAR, not LARGEINT.
    // =========================================================

    sql """drop materialized view if exists test_ivm_varchar_rowid_linear_mv;"""
    sql """drop table if exists test_ivm_varchar_rowid_linear_base;"""

    sql """
        CREATE TABLE test_ivm_varchar_rowid_linear_base (
            k1 VARCHAR(20),
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
        INSERT INTO test_ivm_varchar_rowid_linear_base VALUES
            ('a', 10),
            ('b', 20),
            (NULL, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_varchar_rowid_linear_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM test_ivm_varchar_rowid_linear_base WHERE v1 > 0;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_linear_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_linear_mv")

    // Row-id column keeps the VARCHAR key type (not hashed to largeint)
    sql """SET show_hidden_columns = true;"""
    qt_varchar_linear_desc_initial """DESC test_ivm_varchar_rowid_linear_mv"""
    sql """SET show_hidden_columns = false;"""

    order_qt_varchar_linear_initial """SELECT k1, v1 FROM test_ivm_varchar_rowid_linear_mv"""

    // Upsert the NULL-key row: (NULL, 30) -> (NULL, 99)
    sql """INSERT INTO test_ivm_varchar_rowid_linear_base VALUES (NULL, 99);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_linear_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_linear_mv")

    order_qt_varchar_linear_after_update """SELECT k1, v1 FROM test_ivm_varchar_rowid_linear_mv"""

    // Delete the NULL-key row and add a new VARCHAR key row
    sql """DELETE FROM test_ivm_varchar_rowid_linear_base WHERE k1 IS NULL;"""
    sql """INSERT INTO test_ivm_varchar_rowid_linear_base VALUES ('c', 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_linear_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_linear_mv")

    order_qt_varchar_linear_after_delete """SELECT k1, v1 FROM test_ivm_varchar_rowid_linear_mv"""

    // Final COMPLETE refresh rebuilds the MV; the row-id layout and data must stay identical
    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_linear_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_linear_mv")

    sql """SET show_hidden_columns = true;"""
    qt_varchar_linear_desc_after_complete """DESC test_ivm_varchar_rowid_linear_mv"""
    sql """SET show_hidden_columns = false;"""
    order_qt_varchar_linear_after_complete """SELECT k1, v1 FROM test_ivm_varchar_rowid_linear_mv"""

    sql """drop materialized view if exists test_ivm_varchar_rowid_linear_mv;"""
    sql """drop table if exists test_ivm_varchar_rowid_linear_base;"""

    // =========================================================
    // Part 2: Aggregated MV GROUP BY a single VARCHAR key that
    // contains a NULL group. The group row-id is the VARCHAR key
    // itself and the NULL-key group has a NULL row-id; delta
    // apply must match it null-safely.
    // =========================================================

    sql """drop materialized view if exists test_ivm_varchar_rowid_agg_mv;"""
    sql """drop table if exists test_ivm_varchar_rowid_agg_base;"""

    sql """
        CREATE TABLE test_ivm_varchar_rowid_agg_base (
            k1 VARCHAR(20),
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

    // Groups: k1='a', k1='b' and a NULL-key group
    sql """
        INSERT INTO test_ivm_varchar_rowid_agg_base VALUES
            ('a', 10),
            ('b', 20),
            (NULL, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_varchar_rowid_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM test_ivm_varchar_rowid_agg_base GROUP BY k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_agg_mv")

    // Row-id column keeps the VARCHAR key type (not hashed to largeint)
    sql """SET show_hidden_columns = true;"""
    qt_varchar_agg_desc_initial """DESC test_ivm_varchar_rowid_agg_mv"""
    sql """SET show_hidden_columns = false;"""

    // NULL-key group materializes as its own row (NULL row-id)
    order_qt_varchar_agg_initial """SELECT k1, cnt, sum_v1 FROM test_ivm_varchar_rowid_agg_mv"""

    // Update the NULL-key group via MOW upsert: (NULL, 30) -> (NULL, 99)
    sql """INSERT INTO test_ivm_varchar_rowid_agg_base VALUES (NULL, 99);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_agg_mv")

    order_qt_varchar_agg_after_update """SELECT k1, cnt, sum_v1 FROM test_ivm_varchar_rowid_agg_mv"""

    // Mix: delete the NULL-key group and add a new non-NULL group in one refresh
    sql """DELETE FROM test_ivm_varchar_rowid_agg_base WHERE k1 IS NULL;"""
    sql """INSERT INTO test_ivm_varchar_rowid_agg_base VALUES ('c', 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_agg_mv")

    order_qt_varchar_agg_after_delete """SELECT k1, cnt, sum_v1 FROM test_ivm_varchar_rowid_agg_mv"""

    // Final COMPLETE refresh rebuilds the MV; the row-id layout and data must stay identical
    sql """REFRESH MATERIALIZED VIEW test_ivm_varchar_rowid_agg_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_varchar_rowid_agg_mv")

    sql """SET show_hidden_columns = true;"""
    qt_varchar_agg_desc_after_complete """DESC test_ivm_varchar_rowid_agg_mv"""
    sql """SET show_hidden_columns = false;"""
    order_qt_varchar_agg_after_complete """SELECT k1, cnt, sum_v1 FROM test_ivm_varchar_rowid_agg_mv"""

    sql """drop materialized view if exists test_ivm_varchar_rowid_agg_mv;"""
    sql """drop table if exists test_ivm_varchar_rowid_agg_base;"""
}
