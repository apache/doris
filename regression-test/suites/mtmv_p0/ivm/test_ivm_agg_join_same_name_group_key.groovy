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

suite("test_ivm_agg_join_same_name_group_key") {

    // GROUP BY l.id, r.id over a join of two tables that both expose a column "id": the
    // same-named group keys must keep their identity through the aggregate delta rewrite.
    // A name-keyed lookup collapses both keys onto one slot and turns group (1,20) into
    // (20,20), diverging from COMPLETE.

    def mvName = "ivm_same_key_mv"
    def lTable = "ivm_same_key_l"
    def rTable = "ivm_same_key_r"

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${lTable}"""
    sql """drop table if exists ${rTable}"""

    // Both tables carry a column literally named "id"; the join key is k.
    sql """
        CREATE TABLE ${lTable} (
            id INT,
            k INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE ${rTable} (
            id INT,
            k INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """INSERT INTO ${lTable} VALUES (1,1,100),(2,2,200),(3,3,300)"""
    sql """INSERT INTO ${rTable} VALUES (10,1),(20,2),(30,3)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${lTable}.id AS lid,
               ${rTable}.id AS rid,
               SUM(${lTable}.v) AS total,
               COUNT(*) AS cnt
        FROM ${lTable}
        INNER JOIN ${rTable}
            ON ${lTable}.k = ${rTable}.k
        GROUP BY ${lTable}.id, ${rTable}.id
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_same_key_complete """SELECT lid, rid, total, cnt FROM ${mvName}"""

    // Incremental delta: a brand-new group (lid=4, rid=40). Under the same-name collapse
    // the lid value would be taken from the rid slot and the group would be written as
    // (40,40), so this assert is the regression guard.
    sql """INSERT INTO ${lTable} VALUES (4,4,400)"""
    sql """INSERT INTO ${rTable} VALUES (40,4)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_same_key_incremental """SELECT lid, rid, total, cnt FROM ${mvName}"""

    // Cross-check against a fresh COMPLETE rebuild.
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_same_key_complete2 """SELECT lid, rid, total, cnt FROM ${mvName}"""

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${lTable}"""
    sql """drop table if exists ${rTable}"""

    // =========================================================
    // Part 2: same-named group keys with ivm_use_full_keys=true
    // (MV key = row_id + group by keys)
    // =========================================================
    def fkMv = "ivm_same_key_fk_mv"
    def fkL = "ivm_same_key_fk_l"
    def fkR = "ivm_same_key_fk_r"

    sql """drop materialized view if exists ${fkMv}"""
    sql """drop table if exists ${fkL}"""
    sql """drop table if exists ${fkR}"""

    sql """
        CREATE TABLE ${fkL} (
            id INT,
            k INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE ${fkR} (
            id INT,
            k INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """INSERT INTO ${fkL} VALUES (1,1,100),(2,2,200),(3,3,300)"""
    sql """INSERT INTO ${fkR} VALUES (10,1),(20,2),(30,3)"""

    sql """
        CREATE MATERIALIZED VIEW ${fkMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1', 'ivm_use_full_keys' = 'true')
        AS
        SELECT ${fkL}.id AS lid,
               ${fkR}.id AS rid,
               SUM(${fkL}.v) AS total,
               COUNT(*) AS cnt
        FROM ${fkL}
        INNER JOIN ${fkR}
            ON ${fkL}.k = ${fkR}.k
        GROUP BY ${fkL}.id, ${fkR}.id
    """

    sql """REFRESH MATERIALIZED VIEW ${fkMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(fkMv)
    order_qt_same_key_fk_complete """SELECT lid, rid, total, cnt FROM ${fkMv}"""

    sql """INSERT INTO ${fkL} VALUES (4,4,400)"""
    sql """INSERT INTO ${fkR} VALUES (40,4)"""
    sql """REFRESH MATERIALIZED VIEW ${fkMv} INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName(fkMv)
    order_qt_same_key_fk_incremental """SELECT lid, rid, total, cnt FROM ${fkMv}"""

    sql """drop materialized view if exists ${fkMv}"""
    sql """drop table if exists ${fkL}"""
    sql """drop table if exists ${fkR}"""

    // =========================================================
    // Part 3: same-named group keys under UPDATE (delete + insert delta)
    // =========================================================
    def upMv = "ivm_same_key_up_mv"
    def upL = "ivm_same_key_up_l"
    def upR = "ivm_same_key_up_r"

    sql """drop materialized view if exists ${upMv}"""
    sql """drop table if exists ${upL}"""
    sql """drop table if exists ${upR}"""

    sql """
        CREATE TABLE ${upL} (
            id INT,
            k INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE ${upR} (
            id INT,
            k INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """INSERT INTO ${upL} VALUES (1,1,100),(2,2,200),(3,3,300)"""
    sql """INSERT INTO ${upR} VALUES (10,1),(20,2),(30,3)"""

    sql """
        CREATE MATERIALIZED VIEW ${upMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ${upL}.id AS lid,
               ${upR}.id AS rid,
               SUM(${upL}.v) AS total,
               COUNT(*) AS cnt
        FROM ${upL}
        INNER JOIN ${upR}
            ON ${upL}.k = ${upR}.k
        GROUP BY ${upL}.id, ${upR}.id
    """

    sql """REFRESH MATERIALIZED VIEW ${upMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(upMv)
    order_qt_same_key_up_complete """SELECT lid, rid, total, cnt FROM ${upMv}"""

    // MOW update on l: (1,1,100) -> (1,5,50) drops the join with r(10,1) and forms a new
    // group (1,50). The incremental delta deletes group (1,10) and inserts group (1,50);
    // a same-name collapse would keep (1,10) and/or write the new key wrongly.
    sql """INSERT INTO ${upL} VALUES (1,5,50)"""
    sql """INSERT INTO ${upR} VALUES (50,5)"""
    sql """REFRESH MATERIALIZED VIEW ${upMv} INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName(upMv)
    order_qt_same_key_up_incremental """SELECT lid, rid, total, cnt FROM ${upMv}"""

    sql """drop materialized view if exists ${upMv}"""
    sql """drop table if exists ${upL}"""
    sql """drop table if exists ${upR}"""
}
