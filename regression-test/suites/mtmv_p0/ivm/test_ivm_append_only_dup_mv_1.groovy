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

suite("test_ivm_append_only_dup_mv_1") {
    // An append-only detail IVM MV: every base table is an append-only DUP detail table, and the query
    // holds no aggregate and no outer join. The MV is then a plain DUP table -- no IVM hidden column, no
    // merge-on-write, no plan signature -- and its refresh only ever appends what the base tables appended.
    // This suite covers the table model itself and the single-table operators such an MV supports -- scan,
    // project, filter and subquery alias -- plus the rows the query's own shape produces, duplicates
    // included. The operators that combine two inputs are in _2; chained MVs, the model a user can ask for
    // and a broken premise are in _3.

    // =====================================================================================
    // Part 1: single table -- scan, project, filter, subquery alias, and the table model
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_single;"""
    sql """drop table if exists dup_base_single;"""
    sql """
        CREATE TABLE dup_base_single (
            id BIGINT NOT NULL,
            category VARCHAR(20) NOT NULL,
            amount DECIMAL(12, 2) NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        INSERT INTO dup_base_single VALUES
            (1, 'book', 10.00),
            (2, 'food', 20.00),
            (3, 'toy', 5.00);
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_single
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT id, category, amount
        FROM (SELECT id, category, amount FROM dup_base_single WHERE amount >= 10) t;
    """

    // The table model, which decides everything else: `desc` reports the query's own columns and the key
    // type of a plain DUP table.
    qt_dup_single_schema """desc dup_mv_single all"""

    // SHOW CREATE is what a user rebuilds the MV from, so it has to name the model it really is.
    def dupSingleDdl = sql """SHOW CREATE MATERIALIZED VIEW dup_mv_single"""
    logger.info("show create dup_mv_single: " + dupSingleDdl.toString())
    assertTrue(dupSingleDdl.toString().contains("DUPLICATE KEY("))
    assertFalse(dupSingleDdl.toString().contains("enable_unique_key_merge_on_write"))

    // A column name Doris reserves for a hidden one is rejected here as well: the MV is built with the
    // query's own columns, and a `__DORIS_`-named one is not a column a user can see, so accepting it would
    // leave the MV holding fewer columns than its query produced. The unique-key model rejects these names
    // too, and both models lay hidden columns on the MV they build.
    test {
        sql """CREATE MATERIALIZED VIEW dup_mv_reserved_alias
                BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES ("replication_num" = "1")
                AS SELECT id AS __DORIS_IVM_USER_COL__, category FROM dup_base_single"""
        exception "column name can't start with '__DORIS_'"
    }

    sql """REFRESH MATERIALIZED VIEW dup_mv_single COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_single")
    order_qt_dup_single_after_complete """SELECT id, category, amount FROM dup_mv_single"""

    // The delta is what the base table appended: the filter still applies to it.
    sql """INSERT INTO dup_base_single VALUES (4, 'book', 30.00), (5, 'toy', 3.00);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_single INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_single")
    order_qt_dup_single_after_incremental """SELECT id, category, amount FROM dup_mv_single"""

    // A complete refresh rebuilds the same rows the appends produced.
    sql """REFRESH MATERIALIZED VIEW dup_mv_single COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_single")
    order_qt_dup_single_after_complete_again """SELECT id, category, amount FROM dup_mv_single"""

    // A partitioned detail MV: the MV is a plain DUP table, so its partition column may be one of its value
    // columns -- the rule that a partition column has to be a key column is the merge-on-write model's, and
    // the partition validation used to apply it to every IVM MV.
    sql """drop materialized view if exists dup_mv_partition;"""
    sql """drop table if exists dup_base_partition;"""
    sql """
        CREATE TABLE dup_base_partition (
            id BIGINT NOT NULL,
            dt DATE NOT NULL,
            v1 INT NOT NULL
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(dt) (PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01')))
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_partition
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(id)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, dt, v1 FROM dup_base_partition;
    """
    qt_dup_partition_schema """desc dup_mv_partition all"""

    sql """INSERT INTO dup_base_partition VALUES (1, '2026-01-05', 10);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_partition COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_partition")
    order_qt_dup_partition_after_complete """SELECT id, dt, v1 FROM dup_mv_partition"""

    sql """INSERT INTO dup_base_partition VALUES (2, '2026-01-06', 20);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_partition INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_partition")
    order_qt_dup_partition_after_incremental """SELECT id, dt, v1 FROM dup_mv_partition"""

    // =====================================================================================
    // Part 2: repeated rows -- the MV holds the query's own rows, duplicates included
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_dup_rows;"""
    sql """drop table if exists dup_base_dup_rows;"""
    sql """
        CREATE TABLE dup_base_dup_rows (
            id BIGINT NOT NULL,
            v1 INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_dup_rows
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, v1 FROM dup_base_dup_rows;
    """
    sql """INSERT INTO dup_base_dup_rows VALUES (1, 7), (1, 7);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_dup_rows COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_dup_rows")
    order_qt_dup_rows_after_complete """SELECT id, v1, count(*) AS c FROM dup_mv_dup_rows GROUP BY id, v1"""

    sql """INSERT INTO dup_base_dup_rows VALUES (2, 7), (2, 7), (2, 7);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_dup_rows INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_dup_rows")
    order_qt_dup_rows_after_incremental """SELECT id, v1, count(*) AS c FROM dup_mv_dup_rows GROUP BY id, v1"""
}
