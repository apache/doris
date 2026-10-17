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

suite("test_row_ttl_direct_compaction", "nonConcurrent") {
    // Ablate vertical compaction: the same direct-expiration workload must produce the same
    // visible rows and physical reclamation with either reader implementation.
    for (boolean vertical : [false, true]) {
        setBeConfigTemporary([
            enable_vertical_compaction: vertical,
            vertical_compaction_num_columns_per_group: 2
        ]) {
            sql "DROP TABLE IF EXISTS row_ttl_direct_gc"
            sql """
                CREATE TABLE row_ttl_direct_gc (k INT, v1 INT, v2 INT, v3 INT) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (
                    "replication_num"="1", "disable_auto_compaction"="true",
                    "function_column.enable_row_ttl"="true")
            """
            sql "INSERT INTO row_ttl_direct_gc(k,v1,v2,v3,__DORIS_TTL_COL__) VALUES (1,1,1,1,0)"
            sql """INSERT INTO row_ttl_direct_gc(k,v1,v2,v3,__DORIS_TTL_COL__) VALUES
                (2,2,2,2,9223372036854775807), (3,3,3,3,NULL)"""
            "order_qt_gc_before_${vertical}"("SELECT * FROM row_ttl_direct_gc ORDER BY k")
            trigger_and_wait_compaction("row_ttl_direct_gc", "full")
            "order_qt_gc_after_${vertical}"("SELECT * FROM row_ttl_direct_gc ORDER BY k")
            if (!isCloudMode()) {
                def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_direct_gc")[0].MetaUrl
                def (code, out, err) = curl("GET", metaUrl)
                def physicalRows = parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                "qt_gc_physical_${vertical}"("SELECT ${physicalRows} AS physical_rows")
            }

            sql "DROP TABLE IF EXISTS row_ttl_direct_mor_gc"
            sql """
                CREATE TABLE row_ttl_direct_mor_gc (k INT, v1 INT, v2 INT, v3 INT) UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (
                    "replication_num"="1", "disable_auto_compaction"="true",
                    "enable_unique_key_merge_on_write"="false", "function_column.enable_row_ttl"="true")
            """
            sql """INSERT INTO row_ttl_direct_mor_gc(k,v1,v2,v3,__DORIS_TTL_COL__) VALUES
                (1,1,1,1,9223372036854775807), (2,2,2,2,NULL)"""
            sql "INSERT INTO row_ttl_direct_mor_gc VALUES (3,3,3,3)"
            trigger_and_wait_compaction("row_ttl_direct_mor_gc", "full")
            // Expired latest values must continue masking the live older version after cumulative GC.
            for (int value : 4..8) {
                sql "INSERT INTO row_ttl_direct_mor_gc(k,v1,v2,v3,__DORIS_TTL_COL__) VALUES (1,${value},0,0,0)"
            }
            "order_qt_mor_before_${vertical}"("SELECT * FROM row_ttl_direct_mor_gc ORDER BY k")
            trigger_and_wait_compaction("row_ttl_direct_mor_gc", "cumulative")
            "order_qt_mor_cumulative_${vertical}"("SELECT * FROM row_ttl_direct_mor_gc ORDER BY k")
            trigger_and_wait_compaction("row_ttl_direct_mor_gc", "full")
            "order_qt_mor_full_${vertical}"("SELECT * FROM row_ttl_direct_mor_gc ORDER BY k")

            sql "DROP TABLE IF EXISTS row_ttl_direct_mow_gc"
            sql """
                CREATE TABLE row_ttl_direct_mow_gc (k INT, v1 INT, v2 INT, v3 INT) UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (
                    "replication_num"="1", "disable_auto_compaction"="true",
                    "enable_unique_key_merge_on_write"="true", "function_column.enable_row_ttl"="true")
            """
            sql """INSERT INTO row_ttl_direct_mow_gc(k,v1,v2,v3,__DORIS_TTL_COL__) VALUES
                (1,1,1,1,9223372036854775807), (2,2,2,2,NULL)"""
            sql "INSERT INTO row_ttl_direct_mow_gc(k,v1,v2,v3,__DORIS_TTL_COL__) VALUES (1,3,3,3,0)"
            trigger_and_wait_compaction("row_ttl_direct_mow_gc", "full")
            "order_qt_mow_full_${vertical}"("SELECT * FROM row_ttl_direct_mow_gc ORDER BY k")
        }
    }

    sql "DROP TABLE IF EXISTS row_ttl_direct_maintenance"
    sql """
        CREATE TABLE row_ttl_direct_maintenance (k INT, sub_k INT, v INT) UNIQUE KEY(k,sub_k)
        PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (10))
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (
            "replication_num"="1", "enable_unique_key_merge_on_write"="true",
            "light_schema_change"="true", "function_column.enable_row_ttl"="true")
    """
    sql """INSERT INTO row_ttl_direct_maintenance(k,sub_k,v,__DORIS_TTL_COL__) VALUES
        (1,1,1,0), (2,2,2,4070908800123456), (3,3,3,NULL)"""
    sql "ALTER TABLE row_ttl_direct_maintenance ADD PARTITION p2 VALUES LESS THAN (20)"
    sql "INSERT INTO row_ttl_direct_maintenance(k,sub_k,v,__DORIS_TTL_COL__) VALUES (11,11,11,0),(12,12,12,NULL)"
    sql "ALTER TABLE row_ttl_direct_maintenance ADD COLUMN extra INT DEFAULT '7'"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName = 'row_ttl_direct_maintenance' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    sql "ALTER TABLE row_ttl_direct_maintenance ADD ROLLUP row_ttl_direct_rollup(sub_k,k,v,extra)"
    waitingMVTaskFinishedByMvName(context.dbName, "row_ttl_direct_maintenance", "row_ttl_direct_rollup")
    order_qt_schema_change "SELECT k,sub_k,v,extra FROM row_ttl_direct_maintenance ORDER BY k"
    order_qt_rollup """SELECT /*+ USE_MV(row_ttl_direct_maintenance.row_ttl_direct_rollup) */ k,sub_k,v,extra
        FROM row_ttl_direct_maintenance ORDER BY k"""
    sql "SET enable_unique_key_partial_update = true"
    try {
        sql "INSERT INTO row_ttl_direct_maintenance(k,sub_k,v) VALUES (2,2,20)"
    } finally {
        sql "SET enable_unique_key_partial_update = false"
    }
    sql "SET show_hidden_columns = true"
    try {
        order_qt_rollup_partial """SELECT /*+ USE_MV(row_ttl_direct_maintenance.row_ttl_direct_rollup) */
            k,v,__DORIS_TTL_COL__ FROM row_ttl_direct_maintenance ORDER BY k"""
    } finally {
        sql "SET show_hidden_columns = false"
    }
}
