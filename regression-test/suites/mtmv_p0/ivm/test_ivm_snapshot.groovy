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

suite("test_ivm_snapshot") {
    sql """drop materialized view if exists test_ivm_snapshot_mv;"""
    sql """drop table if exists test_ivm_snapshot_base;"""

    // 1. Create base table (MOW UNIQUE KEYS)
    sql """
        CREATE TABLE test_ivm_snapshot_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.need_historical_value" = "true"
        );
    """

    // 2. Create IVM MV (BUILD DEFERRED, REFRESH INCREMENTAL, ON MANUAL)
    sql """
        CREATE MATERIALIZED VIEW test_ivm_snapshot_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM test_ivm_snapshot_base;
    """

    // Verify MV state is INIT
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}')
        where Name='test_ivm_snapshot_mv'"""
    assertTrue(mvInfos.toString().contains("INIT"))

    // 3. COMPLETE refresh → MV catchup with base table
    sql """REFRESH MATERIALIZED VIEW test_ivm_snapshot_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_snapshot_mv")
    advance_ivm_stream_offset("test_ivm_snapshot_mv")

    def syncAfterComplete = sql """select SyncWithBaseTables from mv_infos('database'='${context.dbName}')
        where Name='test_ivm_snapshot_mv'"""
    logger.info("syncAfterComplete: " + syncAfterComplete.toString())
    assertTrue(syncAfterComplete.toString().contains("true"))

    // 4. Insert a row into base table → MV not catchup
    sql """INSERT INTO test_ivm_snapshot_base VALUES (1, 10);"""

    def syncAfterInsert1 = sql """select SyncWithBaseTables from mv_infos('database'='${context.dbName}')
        where Name='test_ivm_snapshot_mv'"""
    logger.info("syncAfterInsert1: " + syncAfterInsert1.toString())
    assertTrue(syncAfterInsert1.toString().contains("false"))

    // 5. IVM INCREMENTAL refresh → MV catchup
    sql """REFRESH MATERIALIZED VIEW test_ivm_snapshot_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_snapshot_mv")

    def syncAfterIncr = sql """select SyncWithBaseTables from mv_infos('database'='${context.dbName}')
        where Name='test_ivm_snapshot_mv'"""
    logger.info("syncAfterIncr: " + syncAfterIncr.toString())
    assertTrue(syncAfterIncr.toString().contains("true"))

    order_qt_after_incr """SELECT k1, v1 FROM test_ivm_snapshot_mv"""

    // 6. Insert another row → MV not catchup
    sql """INSERT INTO test_ivm_snapshot_base VALUES (2, 20);"""

    def syncAfterInsert2 = sql """select SyncWithBaseTables from mv_infos('database'='${context.dbName}')
        where Name='test_ivm_snapshot_mv'"""
    logger.info("syncAfterInsert2: " + syncAfterInsert2.toString())
    assertTrue(syncAfterInsert2.toString().contains("false"))

    // 7. COMPLETE refresh → MV catchup
    sql """REFRESH MATERIALIZED VIEW test_ivm_snapshot_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_snapshot_mv")
    advance_ivm_stream_offset("test_ivm_snapshot_mv")

    def syncAfterComplete2 = sql """select SyncWithBaseTables from mv_infos('database'='${context.dbName}')
        where Name='test_ivm_snapshot_mv'"""
    logger.info("syncAfterComplete2: " + syncAfterComplete2.toString())
    assertTrue(syncAfterComplete2.toString().contains("true"))

    order_qt_after_complete """SELECT k1, v1 FROM test_ivm_snapshot_mv"""
}
