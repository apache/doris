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

suite("test_ivm_full_keys_same_name_unprojected_key") {

    // GROUP BY l.id, r.id over a join where both tables expose "id", while the user output
    // selects only the left id (right id is an identity key that must be materialized as a
    // hidden key column). With ivm_use_full_keys the MV key is row_id + group by keys;
    // treating the same-named right id as already projected drops it from the key set.

    def mvName = "ivm_fk_hidden_mv"
    def lTable = "ivm_fk_hidden_l"
    def rTable = "ivm_fk_hidden_r"

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${lTable}"""
    sql """drop table if exists ${rTable}"""

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

    // Only l.id is selected; r.id participates in GROUP BY but is not in the output, so it
    // must be carried by a hidden key column under ivm_use_full_keys.
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1', 'ivm_use_full_keys' = 'true')
        AS
        SELECT ${lTable}.id AS lid,
               SUM(${lTable}.v) AS total,
               COUNT(*) AS cnt
        FROM ${lTable}
        INNER JOIN ${rTable}
            ON ${lTable}.k = ${rTable}.k
        GROUP BY ${lTable}.id, ${rTable}.id
    """

    // The hidden key column for the unprojected r.id group key must exist in the MV schema
    // (hidden columns are hidden from DESC unless show_hidden_columns is on).
    sql """set show_hidden_columns = true"""
    qt_fk_hidden_desc """DESC ${mvName}"""
    sql """set show_hidden_columns = false"""

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_fk_hidden_complete """SELECT lid, total, cnt FROM ${mvName}"""

    sql """INSERT INTO ${lTable} VALUES (4,4,400)"""
    sql """INSERT INTO ${rTable} VALUES (40,4)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_fk_hidden_incremental """SELECT lid, total, cnt FROM ${mvName}"""

    // Cross-check against a fresh COMPLETE rebuild.
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_fk_hidden_complete2 """SELECT lid, total, cnt FROM ${mvName}"""

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${lTable}"""
    sql """drop table if exists ${rTable}"""
}
