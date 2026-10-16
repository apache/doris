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

suite("test_ivm_append_only_dup_mv_3") {
    // What the append-only detail model does across models: an MV chained on a detail MV (both to another
    // detail MV and to a unique-key one), the property a user has to ask for the other table model by name,
    // and what a refresh does when the MV's premise -- every base table is an append-only detail table --
    // stops holding. The table model itself and the single-table operators are in _1; the operators that
    // combine two inputs, UNION ALL included, are in _2.

    // =====================================================================================
    // Part 1: an MV over a detail MV -- the chain stays append-only, and it can cross models
    // =====================================================================================
    // A detail MV is a DUP table too, so an MV over it is judged the same way: the chain keeps appending at
    // both levels, and neither level carries an IVM hidden column. The upstream MV has to carry row binlog
    // itself (binlog.enable on the MV) for the downstream one to read what it appended.
    sql """drop materialized view if exists dup_mv_chained;"""
    sql """drop materialized view if exists dup_mv_chained_mow;"""
    sql """drop materialized view if exists dup_mv_chain_upstream;"""
    sql """drop table if exists dup_base_chain;"""
    sql """
        CREATE TABLE dup_base_chain (
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
        CREATE MATERIALIZED VIEW dup_mv_chain_upstream
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
        AS SELECT id, v1 FROM dup_base_chain;
    """
    sql """INSERT INTO dup_base_chain VALUES (1, 10), (2, 20), (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_chain_upstream COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chain_upstream")
    order_qt_dup_chain_upstream_after_complete """SELECT id, v1 FROM dup_mv_chain_upstream"""

    // The MV over it is a detail MV as well: the chain appends at both levels.
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chained
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
        AS SELECT id, v1 FROM dup_mv_chain_upstream WHERE v1 >= 20;
    """
    qt_dup_chained_schema """desc dup_mv_chained all"""

    sql """REFRESH MATERIALIZED VIEW dup_mv_chained COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained")
    order_qt_dup_chained_after_complete """SELECT id, v1 FROM dup_mv_chained"""

    // Upstream first, then downstream: the downstream delta is built from the upstream MV's own appended
    // rows.
    sql """INSERT INTO dup_base_chain VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_chain_upstream INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chain_upstream")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained")
    order_qt_dup_chained_after_incremental """SELECT id, v1 FROM dup_mv_chained"""

    // A downstream MV whose query aggregates is a unique-key MV even though its base table is the detail MV
    // above, so the chain spans both models: the downstream one carries the layout the unique-key model
    // needs, and its delta is built from the rows the detail MV appended.
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chained_mow
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, COUNT(*) AS cnt, SUM(v1) AS sum_v1 FROM dup_mv_chain_upstream GROUP BY id;
    """
    qt_dup_chained_mow_schema """desc dup_mv_chained_mow all"""

    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_mow COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_mow")
    order_qt_dup_chained_mow_after_complete """SELECT id, cnt, sum_v1 FROM dup_mv_chained_mow"""

    // Upstream first, then the unique-key downstream one.
    sql """INSERT INTO dup_base_chain VALUES (6, 60);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_chain_upstream INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chain_upstream")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_mow INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_mow")
    order_qt_dup_chained_mow_after_incremental """SELECT id, cnt, sum_v1 FROM dup_mv_chained_mow"""

    // A third level on top of that: an MV over a detail MV that is itself over a detail MV. The chain stays
    // append-only at every level, since the level-2 MV now carries row binlog of its own.
    sql """drop materialized view if exists dup_mv_chained_l3;"""
    sql """drop materialized view if exists dup_mv_chained_l3_mow;"""
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chained_l3
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
        AS SELECT id, v1 FROM dup_mv_chained;
    """
    qt_dup_chained_l3_schema """desc dup_mv_chained_l3 all"""

    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_l3 COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_l3")
    order_qt_dup_chained_l3_after_complete """SELECT id, v1 FROM dup_mv_chained_l3"""

    // One append at the bottom of the chain, refreshed level by level.
    sql """INSERT INTO dup_base_chain VALUES (7, 70);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_chain_upstream INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chain_upstream")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_l3 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_l3")
    order_qt_dup_chained_l3_after_incremental """SELECT id, v1 FROM dup_mv_chained_l3"""

    // A unique-key MV at the end of the same chain: it aggregates, so the last step crosses models, and
    // the MV it reads carries row binlog of its own for exactly that reason.
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chained_l3_mow
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, COUNT(*) AS cnt FROM dup_mv_chained_l3 GROUP BY id;
    """
    qt_dup_chained_l3_mow_schema """desc dup_mv_chained_l3_mow all"""

    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_l3_mow COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_l3_mow")
    order_qt_dup_chained_l3_mow_after_complete """SELECT id, cnt FROM dup_mv_chained_l3_mow"""

    sql """INSERT INTO dup_base_chain VALUES (8, 80);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_chain_upstream INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chain_upstream")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_l3 INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_l3")
    sql """REFRESH MATERIALIZED VIEW dup_mv_chained_l3_mow INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_chained_l3_mow")
    order_qt_dup_chained_l3_mow_after_incremental """SELECT id, cnt FROM dup_mv_chained_l3_mow"""

    // The upstream MV has to carry row binlog itself for a downstream one to read what it appended: without
    // it there is no stream to read, and CREATE has to say so rather than build an MV that cannot refresh.
    sql """drop materialized view if exists dup_mv_chain_nobln;"""
    sql """
        CREATE MATERIALIZED VIEW dup_mv_chain_nobln
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, v1 FROM dup_base_chain;
    """
    test {
        sql """CREATE MATERIALIZED VIEW dup_mv_chain_nobln_down
                BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES ("replication_num" = "1")
                AS SELECT id, v1 FROM dup_mv_chain_nobln"""
        exception "row binlog is not enabled for table"
    }

    // =====================================================================================
    // Part 2: ivm_force_mow -- the other model, asked for by name
    // =====================================================================================
    // The same query over the same append-only detail base table, built on the unique-key model because the
    // MV was created with ivm_force_mow. The property is the user's way to ask for the model that retracts;
    // like ivm_use_full_keys it is fixed at creation, so ALTER reports it instead of moving the MV to a
    // model its refresh would contradict.
    sql """drop materialized view if exists dup_mv_forced_mow;"""
    sql """drop table if exists dup_base_forced_mow;"""
    sql """
        CREATE TABLE dup_base_forced_mow (
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
        INSERT INTO dup_base_forced_mow VALUES
            (1, 'book', 10.00),
            (2, 'food', 20.00),
            (3, 'toy', 5.00);
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_forced_mow
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "ivm_force_mow" = "true"
        )
        AS SELECT id, category, amount FROM dup_base_forced_mow WHERE amount >= 10;
    """
    qt_dup_forced_mow_schema """desc dup_mv_forced_mow all"""

    // SHOW CREATE is what a user rebuilds the MV from, so it has to name the model it really is: the
    // unique-key one prints KEY rather than the DUPLICATE KEY of the model the query would have settled on.
    def forcedMowDdl = sql """SHOW CREATE MATERIALIZED VIEW dup_mv_forced_mow"""
    logger.info("show create dup_mv_forced_mow: " + forcedMowDdl.toString())
    assertTrue(forcedMowDdl.toString().contains('KEY(`id`)'))
    assertFalse(forcedMowDdl.toString().contains("DUPLICATE KEY("))

    sql """REFRESH MATERIALIZED VIEW dup_mv_forced_mow COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_forced_mow")
    order_qt_dup_forced_mow_after_complete """SELECT id, category, amount FROM dup_mv_forced_mow"""

    // The delta of that model: the row the base table appended, applied through the row-id.
    sql """INSERT INTO dup_base_forced_mow VALUES (6, 'book', 60.00);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_forced_mow INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_forced_mow")
    order_qt_dup_forced_mow_after_incremental """SELECT id, category, amount FROM dup_mv_forced_mow"""

    test {
        sql """ALTER MATERIALIZED VIEW dup_mv_forced_mow SET ('ivm_force_mow' = 'true')"""
        exception "cannot be altered"
    }

    // =====================================================================================
    // Part 3: the premise broken -- a base table that stops being an append-only detail table
    // =====================================================================================
    // A detail MV can only be maintained over tables whose rows are only ever appended: its delta has no
    // place to carry a retraction. Replacing the base table with a merge-on-write unique table must
    // therefore fail the refresh loudly, rather than silently inserting the rows a delete retracted.
    sql """drop materialized view if exists dup_mv_premise;"""
    sql """drop table if exists dup_base_premise;"""
    sql """drop table if exists dup_base_premise_mow;"""
    sql """
        CREATE TABLE dup_base_premise (
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
    sql """INSERT INTO dup_base_premise VALUES (1, 'book', 10.00);"""
    sql """
        CREATE MATERIALIZED VIEW dup_mv_premise
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS SELECT id, category, amount FROM dup_base_premise WHERE amount >= 10;
    """
    sql """REFRESH MATERIALIZED VIEW dup_mv_premise COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_premise")
    order_qt_dup_premise_after_complete """SELECT id, category, amount FROM dup_mv_premise"""

    sql """
        CREATE TABLE dup_base_premise_mow (
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
    sql """INSERT INTO dup_base_premise_mow VALUES (1, 'book', 10.00);"""
    sql """ALTER TABLE dup_base_premise REPLACE WITH TABLE dup_base_premise_mow PROPERTIES('swap' = 'false');"""
    // The name now belongs to the merge-on-write table, and dirtying it is what makes the next refresh
    // have anything to do.
    sql """INSERT INTO dup_base_premise VALUES (2, 'food', 60.00);"""

    test {
        sql """REFRESH MATERIALIZED VIEW dup_mv_premise INCREMENTAL WITH DRY RUN"""
        exception "can only be built over duplicate-key detail tables"
    }

    // A rebuild would normalize the same way and fail the same way, so the MV is left in the state that
    // says it needs one: what a user reads as "drop it and create it again".
    def premiseState = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'dup_mv_premise'"""
    logger.info("dup_mv_premise state: " + premiseState.toString())
    assertTrue(premiseState.toString().contains("SCHEMA_CHANGE"))
}
