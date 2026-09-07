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

suite("test_ivm_replace_table_reconcile", "mtmv") {
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_replace_table_reconcile_mv"
    sql "DROP TABLE IF EXISTS ivm_replace_table_reconcile_base"
    sql "DROP TABLE IF EXISTS ivm_replace_table_reconcile_new_base"

    sql """
        CREATE TABLE ivm_replace_table_reconcile_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW',
            'binlog.need_historical_value' = 'true'
        )
    """

    sql """
        CREATE TABLE ivm_replace_table_reconcile_new_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW',
            'binlog.need_historical_value' = 'true'
        )
    """

    sql "INSERT INTO ivm_replace_table_reconcile_base VALUES (1, 10), (2, 20)"
    sql "INSERT INTO ivm_replace_table_reconcile_new_base VALUES (3, 30), (4, 40)"

    sql """
        CREATE MATERIALIZED VIEW ivm_replace_table_reconcile_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_replace_table_reconcile_base
    """

    sql "REFRESH MATERIALIZED VIEW ivm_replace_table_reconcile_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_replace_table_reconcile_mv")
    order_qt_before_replace "SELECT k1, v1 FROM ivm_replace_table_reconcile_mv ORDER BY k1"

    sql """
        ALTER TABLE ivm_replace_table_reconcile_base
        REPLACE WITH TABLE ivm_replace_table_reconcile_new_base
        PROPERTIES ('swap' = 'false')
    """

    order_qt_after_replace """
        SELECT Name, State, RefreshState, SyncWithBaseTables
        FROM mv_infos('database'='${context.dbName}')
        WHERE Name = 'ivm_replace_table_reconcile_mv'
    """

    // COMPLETE must recreate the stale IVM stream and refresh from the replacement table.
    sql "REFRESH MATERIALIZED VIEW ivm_replace_table_reconcile_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("ivm_replace_table_reconcile_mv")
    order_qt_after_complete_replace """
        SELECT k1, v1 FROM ivm_replace_table_reconcile_mv ORDER BY k1
    """

    sql "INSERT INTO ivm_replace_table_reconcile_base VALUES (5, 50)"
    sql "REFRESH MATERIALIZED VIEW ivm_replace_table_reconcile_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_replace_table_reconcile_mv")
    order_qt_after_recovery_incremental """
        SELECT k1, v1 FROM ivm_replace_table_reconcile_mv ORDER BY k1
    """
}
