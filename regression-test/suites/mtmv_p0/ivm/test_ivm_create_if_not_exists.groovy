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

suite("test_ivm_create_if_not_exists") {
    // Scenario 1 (P0): the name already belongs to an ORDINARY table.
    // CREATE MATERIALIZED VIEW IF NOT EXISTS must be a full no-op: the pre-existing
    // table must survive (CreateMTMVCommand used to force-drop it during rollback)
    // and no IVM stream may be created.
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_create_if_exists_plain"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_plain"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_base"
    sql """
        CREATE TABLE ivm_create_if_exists_base (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW'
        )
    """
    sql "INSERT INTO ivm_create_if_exists_base VALUES (1, 10), (2, 20)"
    sql """
        CREATE TABLE ivm_create_if_exists_plain (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql "INSERT INTO ivm_create_if_exists_plain VALUES (9, 99)"

    // Must succeed as a no-op, not throw and not drop the ordinary table.
    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS ivm_create_if_exists_plain
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_create_if_exists_base
    """
    qt_plain_table_type """
        SELECT TABLE_TYPE FROM information_schema.tables
        WHERE TABLE_SCHEMA = '${context.dbName}' AND TABLE_NAME = 'ivm_create_if_exists_plain'
    """
    order_qt_plain_data "SELECT k1, v1 FROM ivm_create_if_exists_plain ORDER BY k1"
    qt_plain_stream_count """
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME LIKE '__doris_ivm_stream_%'
          AND BASE_TABLE_NAME = 'ivm_create_if_exists_plain'
    """
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_plain"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_base"

    // Scenario 2 (P0): the name already belongs to an IVM MTMV.
    // Re-running CREATE MATERIALIZED VIEW IF NOT EXISTS must keep the existing MTMV
    // and its stream untouched: the stream query before and after the re-create must
    // return the same stream id (it was not dropped and rebuilt), and incremental
    // refresh afterwards must still work.
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_create_if_exists_mv"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_mv_base"
    sql """
        CREATE TABLE ivm_create_if_exists_mv_base (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW'
        )
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_create_if_exists_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_create_if_exists_mv_base
    """
    def streamIdRows = sql """
        SELECT STREAM_ID FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME LIKE '__doris_ivm_stream_%'
          AND BASE_TABLE_NAME = 'ivm_create_if_exists_mv_base'
    """
    assertEquals(1, streamIdRows.size())
    def streamId = streamIdRows[0][0]

    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS ivm_create_if_exists_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_create_if_exists_mv_base
    """
    // The stream id is a runtime value and must not go into a static .out expectation.
    // Assert it with a runtime query instead: the re-create must keep the very same
    // stream, i.e. the stream was not dropped and rebuilt.
    def unchangedStreamRows = sql """
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME LIKE '__doris_ivm_stream_%'
          AND BASE_TABLE_NAME = 'ivm_create_if_exists_mv_base'
          AND STREAM_ID = ${streamId}
    """
    assertEquals(1L, unchangedStreamRows[0][0])

    sql "INSERT INTO ivm_create_if_exists_mv_base VALUES (1, 10), (2, 20)"
    sql "REFRESH MATERIALIZED VIEW ivm_create_if_exists_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("ivm_create_if_exists_mv")
    sql "INSERT INTO ivm_create_if_exists_mv_base VALUES (3, 30)"
    sql "REFRESH MATERIALIZED VIEW ivm_create_if_exists_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_create_if_exists_mv")
    order_qt_mv_data "SELECT k1, v1 FROM ivm_create_if_exists_mv ORDER BY k1"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_create_if_exists_mv"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_mv_base"

    // Scenario 3: repeated create -> complete refresh -> drop cycles stay healthy:
    // stream provisioning (now under the db write lock, shared with the async
    // IMMEDIATE reconcile path) converges to exactly one stream per refresh and
    // drops cleanly with the MV.
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_create_if_exists_cycle_mv"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_cycle_base"
    sql """
        CREATE TABLE ivm_create_if_exists_cycle_base (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW'
        )
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_create_if_exists_cycle_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_create_if_exists_cycle_base
    """
    sql "INSERT INTO ivm_create_if_exists_cycle_base VALUES (1, 10)"
    sql "REFRESH MATERIALIZED VIEW ivm_create_if_exists_cycle_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("ivm_create_if_exists_cycle_mv")
    qt_cycle1_stream_count """
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME LIKE '__doris_ivm_stream_%'
          AND BASE_TABLE_NAME = 'ivm_create_if_exists_cycle_base'
    """
    order_qt_cycle1_data "SELECT k1, v1 FROM ivm_create_if_exists_cycle_mv ORDER BY k1"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_create_if_exists_cycle_mv"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_cycle_base"

    sql """
        CREATE TABLE ivm_create_if_exists_cycle_base (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW'
        )
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_create_if_exists_cycle_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_create_if_exists_cycle_base
    """
    sql "INSERT INTO ivm_create_if_exists_cycle_base VALUES (2, 20)"
    sql "REFRESH MATERIALIZED VIEW ivm_create_if_exists_cycle_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("ivm_create_if_exists_cycle_mv")
    qt_cycle2_stream_count """
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME LIKE '__doris_ivm_stream_%'
          AND BASE_TABLE_NAME = 'ivm_create_if_exists_cycle_base'
    """
    order_qt_cycle2_data "SELECT k1, v1 FROM ivm_create_if_exists_cycle_mv ORDER BY k1"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_create_if_exists_cycle_mv"
    sql "DROP TABLE IF EXISTS ivm_create_if_exists_cycle_base"
}
