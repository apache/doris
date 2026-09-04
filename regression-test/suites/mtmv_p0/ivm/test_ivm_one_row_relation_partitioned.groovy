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

suite("test_ivm_one_row_relation_partitioned") {

    // A partitioned (FOLLOW_BASE_TABLE) IVM MV may contain a OneRowRelation:
    // the constant side produces no delta, the partition routing comes from the
    // real base table, and insert/update/delete drive the incremental refresh.

    sql """drop materialized view if exists ivm_one_row_partitioned_mv;"""
    sql """drop table if exists ivm_one_row_partitioned_t;"""

    sql """
        CREATE TABLE ivm_one_row_partitioned_t (
            dt DATE NOT NULL,
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(dt, k1)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES LESS THAN ("2026-01-02"),
            PARTITION p2 VALUES LESS THAN ("2026-01-03"),
            PARTITION p3 VALUES LESS THAN ("2026-01-04")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_one_row_partitioned_t VALUES
        ("2026-01-01", 1, 10),
        ("2026-01-02", 2, 20),
        ("2026-01-03", 3, 30);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_partitioned_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT t.dt, t.k1, t.v1 * c.factor AS v
        FROM (SELECT 2 AS factor) c
        CROSS JOIN ivm_one_row_partitioned_t t;
    """

    // Initial INCREMENTAL refresh is forced to COMPLETE for OneRowRelation plans
    // and builds all partitions.
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_partitioned_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_partitioned_mv")
    order_qt_partitioned_initial """SELECT dt, k1, v FROM ivm_one_row_partitioned_mv ORDER BY dt"""
    qt_partitioned_partition_count """SELECT count(*) FROM information_schema.partitions
        WHERE table_name = 'ivm_one_row_partitioned_mv'"""

    // Insert into partition p2: only the p2 row appears in the MV.
    sql """INSERT INTO ivm_one_row_partitioned_t VALUES ("2026-01-02", 4, 40);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_partitioned_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_partitioned_mv")
    order_qt_partitioned_after_insert """SELECT dt, k1, v FROM ivm_one_row_partitioned_mv ORDER BY dt"""

    // Update in partition p1: the p1 row is maintained.
    sql """UPDATE ivm_one_row_partitioned_t SET v1 = 11 WHERE dt = "2026-01-01" AND k1 = 1;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_partitioned_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_partitioned_mv")
    order_qt_partitioned_after_update """SELECT dt, k1, v FROM ivm_one_row_partitioned_mv ORDER BY dt"""

    // Delete in partition p3: the p3 row disappears.
    sql """DELETE FROM ivm_one_row_partitioned_t WHERE dt = "2026-01-03" AND k1 = 3;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_partitioned_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_partitioned_mv")
    order_qt_partitioned_after_delete """SELECT dt, k1, v FROM ivm_one_row_partitioned_mv ORDER BY dt"""

    sql """drop materialized view if exists ivm_one_row_partitioned_mv;"""
    sql """drop table if exists ivm_one_row_partitioned_t;"""

    // Part 2: PCT — the base table starts with one partition and grows via
    // ADD PARTITION; the MV follows automatically on each refresh.
    sql """drop materialized view if exists ivm_one_row_pct_mv;"""
    sql """drop table if exists ivm_one_row_pct_t;"""
    sql """
        CREATE TABLE ivm_one_row_pct_t (
            dt DATE NOT NULL,
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(dt, k1)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES LESS THAN ("2026-01-02")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO ivm_one_row_pct_t VALUES ("2026-01-01", 1, 10);"""
    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_pct_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT t.dt, t.k1, t.v1 * c.factor AS v
        FROM (SELECT 2 AS factor) c
        CROSS JOIN ivm_one_row_pct_t t;
    """
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_pct_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_pct_mv")
    qt_pct_partition_count_after_p1 """SELECT count(*) FROM information_schema.partitions
        WHERE table_name = 'ivm_one_row_pct_mv'"""
    order_qt_pct_after_p1 """SELECT dt, k1, v FROM ivm_one_row_pct_mv ORDER BY dt"""

    // Add partition p2 with data; the MV syncs and maintains it.
    sql """ALTER TABLE ivm_one_row_pct_t ADD PARTITION p2 VALUES LESS THAN ("2026-01-03");"""
    sql """INSERT INTO ivm_one_row_pct_t VALUES ("2026-01-02", 2, 20);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_pct_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_pct_mv")
    qt_pct_partition_count_after_p2 """SELECT count(*) FROM information_schema.partitions
        WHERE table_name = 'ivm_one_row_pct_mv'"""
    order_qt_pct_after_p2 """SELECT dt, k1, v FROM ivm_one_row_pct_mv ORDER BY dt"""

    // Add partition p3 with data; the MV syncs and maintains it.
    sql """ALTER TABLE ivm_one_row_pct_t ADD PARTITION p3 VALUES LESS THAN ("2026-01-04");"""
    sql """INSERT INTO ivm_one_row_pct_t VALUES ("2026-01-03", 3, 30);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_pct_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_pct_mv")
    qt_pct_partition_count_after_p3 """SELECT count(*) FROM information_schema.partitions
        WHERE table_name = 'ivm_one_row_pct_mv'"""
    order_qt_pct_after_p3 """SELECT dt, k1, v FROM ivm_one_row_pct_mv ORDER BY dt"""

    sql """drop materialized view if exists ivm_one_row_pct_mv;"""
    sql """drop table if exists ivm_one_row_pct_t;"""
}
