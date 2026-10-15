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

suite("test_ivm_append_only_dup_mv") {
    // An append-only detail IVM MV: every base table is an append-only DUP detail table, and the query
    // holds no aggregate and no outer join. The MV is then a plain DUP table -- no IVM hidden column, no
    // merge-on-write, no plan signature -- and its refresh only ever appends what the base tables
    // appended. This suite covers the operators such an MV supports: scan, project, filter, subquery
    // alias, inner join, cross join and UNION ALL, plus the table model itself, chained MVs, repeated
    // rows, and what happens when the premise is broken.

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

    // The table model, which decides everything else: a plain DUP table, with no IVM hidden column on it.
    def dupSingleDesc = sql """desc dup_mv_single all"""
    logger.info("desc dup_mv_single: " + dupSingleDesc.toString())
    assertTrue(dupSingleDesc.toString().contains("DUP_KEYS"))
    assertFalse(dupSingleDesc.toString().contains("IVM"))

    // SHOW CREATE is what a user rebuilds the MV from, so it has to name the model it really is.
    def dupSingleDdl = sql """SHOW CREATE MATERIALIZED VIEW dup_mv_single"""
    logger.info("show create dup_mv_single: " + dupSingleDdl.toString())
    assertTrue(dupSingleDdl.toString().contains("DUPLICATE KEY("))
    assertFalse(dupSingleDdl.toString().contains("enable_unique_key_merge_on_write"))

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

    // =====================================================================================
    // Part 3: inner join of two append-only detail tables
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_join;"""
    sql """drop table if exists dup_base_join_l;"""
    sql """drop table if exists dup_base_join_r;"""
    sql """
        CREATE TABLE dup_base_join_l (
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
        CREATE TABLE dup_base_join_r (
            id BIGINT NOT NULL,
            v2 INT NOT NULL
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
        CREATE MATERIALIZED VIEW dup_mv_join
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT l.id AS id, l.v1 AS v1, r.v2 AS v2
        FROM dup_base_join_l l JOIN dup_base_join_r r ON l.id = r.id;
    """
    def dupJoinDesc = sql """desc dup_mv_join all"""
    logger.info("desc dup_mv_join: " + dupJoinDesc.toString())
    assertTrue(dupJoinDesc.toString().contains("DUP_KEYS"))
    assertFalse(dupJoinDesc.toString().contains("IVM"))

    sql """INSERT INTO dup_base_join_l VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO dup_base_join_r VALUES (1, 100), (2, 200), (3, 300);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_after_complete """SELECT id, v1, v2 FROM dup_mv_join"""

    // Only the left table appends: the delta is the left's rows joined against the right's snapshot.
    sql """INSERT INTO dup_base_join_l VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_left_only """SELECT id, v1, v2 FROM dup_mv_join"""

    // Only the right table appends.
    sql """INSERT INTO dup_base_join_r VALUES (4, 400);"""
    sql """INSERT INTO dup_base_join_l VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_right_only """SELECT id, v1, v2 FROM dup_mv_join"""

    // Both sides append inside one window: each side's delta joins the other side's snapshot, and the
    // pair that matches only appears once.
    sql """INSERT INTO dup_base_join_l VALUES (5, 50);"""
    sql """INSERT INTO dup_base_join_r VALUES (5, 500);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_both_sides """SELECT id, v1, v2 FROM dup_mv_join"""

    // A left row that matches nothing contributes nothing, and an id that arrives later still joins.
    sql """INSERT INTO dup_base_join_l VALUES (9, 90);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_unmatched_l """SELECT id, v1, v2 FROM dup_mv_join"""
    sql """INSERT INTO dup_base_join_r VALUES (9, 900);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_late_match """SELECT id, v1, v2 FROM dup_mv_join"""

    // =====================================================================================
    // Part 4: cross join
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_cross;"""
    sql """drop table if exists dup_base_cross_l;"""
    sql """drop table if exists dup_base_cross_r;"""
    sql """
        CREATE TABLE dup_base_cross_l (
            id BIGINT NOT NULL
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
        CREATE TABLE dup_base_cross_r (
            id BIGINT NOT NULL
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
        CREATE MATERIALIZED VIEW dup_mv_cross
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(l_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT l.id AS l_id, r.id AS r_id FROM dup_base_cross_l l CROSS JOIN dup_base_cross_r r;
    """
    sql """INSERT INTO dup_base_cross_l VALUES (1), (2);"""
    sql """INSERT INTO dup_base_cross_r VALUES (10), (20);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_cross COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_cross")
    order_qt_dup_cross_after_complete """SELECT l_id, r_id FROM dup_mv_cross"""

    sql """INSERT INTO dup_base_cross_l VALUES (3);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_cross INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_cross")
    order_qt_dup_cross_after_left_append """SELECT l_id, r_id FROM dup_mv_cross"""

    sql """INSERT INTO dup_base_cross_r VALUES (30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_cross INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_cross")
    order_qt_dup_cross_after_right_append """SELECT l_id, r_id FROM dup_mv_cross"""

    // =====================================================================================
    // Part 5: UNION ALL
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_union;"""
    sql """drop table if exists dup_base_union_a;"""
    sql """drop table if exists dup_base_union_b;"""
    sql """
        CREATE TABLE dup_base_union_a (
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
        CREATE TABLE dup_base_union_b (
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
        CREATE MATERIALIZED VIEW dup_mv_union
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
        AS
        SELECT id, v1 FROM dup_base_union_a
        UNION ALL
        SELECT id, v1 FROM dup_base_union_b;
    """
    sql """INSERT INTO dup_base_union_a VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO dup_base_union_b VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_union COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_union")
    order_qt_dup_union_after_complete """SELECT id, v1 FROM dup_mv_union"""

    sql """INSERT INTO dup_base_union_a VALUES (4, 40);"""
    sql """INSERT INTO dup_base_union_b VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_union INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_union")
    order_qt_dup_union_after_incremental """SELECT id, v1, count(*) AS c FROM dup_mv_union GROUP BY id, v1"""

    // =====================================================================================
    // Part 6: a chained MV stays on the append-only path
    // =====================================================================================
    // The MV above is a DUP table too, so a second MV over it is judged the same way: the whole chain
    // keeps appending, and neither level carries an IVM hidden column.
    sql """drop materialized view if exists dup_mv_chained;"""
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chained
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, v1 FROM dup_mv_union WHERE v1 >= 20;
    """
    def dupChainedDesc = sql """desc dup_mv_chained all"""
    logger.info("desc dup_mv_chained: " + dupChainedDesc.toString())
    assertTrue(dupChainedDesc.toString().contains("DUP_KEYS"))
    assertFalse(dupChainedDesc.toString().contains("IVM"))

    sql """REFRESH MATERIALIZED VIEW dup_mv_chained COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained")
    order_qt_dup_chained_after_complete """SELECT id, v1 FROM dup_mv_chained"""

    // Upstream first, then downstream: the downstream delta is built from the upstream MV's own appended
    // rows.
    sql """INSERT INTO dup_base_union_a VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_union INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_union")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained")
    order_qt_dup_chained_after_incremental """SELECT id, v1 FROM dup_mv_chained"""

    // A downstream MV whose query aggregates is a unique-key MV even though its base table is the detail MV
    // above, so the chain spans both models: the downstream one carries the layout the unique-key model
    // needs, and its delta is built from the rows the detail MV appended.
    sql """drop materialized view if exists dup_mv_chained_mow;"""
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chained_mow
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, COUNT(*) AS cnt, SUM(v1) AS sum_v1 FROM dup_mv_union GROUP BY id;
    """
    def dupChainedMowDesc = sql """desc dup_mv_chained_mow all"""
    logger.info("desc dup_mv_chained_mow: " + dupChainedMowDesc.toString())
    assertTrue(dupChainedMowDesc.toString().contains("UNIQUE_KEYS"))
    assertFalse(dupChainedMowDesc.toString().contains("DUP_KEYS"))

    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_mow COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_mow")
    order_qt_dup_chained_mow_after_complete """SELECT id, cnt, sum_v1 FROM dup_mv_chained_mow"""

    // Upstream first, then the unique-key downstream one.
    sql """INSERT INTO dup_base_union_a VALUES (6, 60);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_union INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_union")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_mow INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_mow")
    order_qt_dup_chained_mow_after_incremental """SELECT id, cnt, sum_v1 FROM dup_mv_chained_mow"""

    // =====================================================================================
    // Part 7: the premise broken -- a base table that stops being an append-only detail table
    // =====================================================================================
    // A detail MV can only be maintained over tables whose rows are only ever appended: its delta has no
    // place to carry a retraction. Replacing the base table with a merge-on-write unique table must
    // therefore fail the refresh loudly, rather than silently inserting the rows a delete retracted.
    sql """drop table if exists dup_base_single_mow;"""
    sql """
        CREATE TABLE dup_base_single_mow (
            id BIGINT NOT NULL,
            category VARCHAR(20) NOT NULL,
            amount DECIMAL(12, 2) NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """
    sql """INSERT INTO dup_base_single_mow VALUES (1, 'book', 10.00);"""
    sql """ALTER TABLE dup_base_single REPLACE WITH TABLE dup_base_single_mow PROPERTIES('swap' = 'false');"""
    // The name now belongs to the merge-on-write table, and dirtying it is what makes the next refresh
    // have anything to do.
    sql """INSERT INTO dup_base_single VALUES (6, 'food', 60.00);"""

    test {
        sql """REFRESH MATERIALIZED VIEW dup_mv_single INCREMENTAL WITH DRY RUN"""
        exception "can only be built over duplicate-key detail tables"
    }
}
