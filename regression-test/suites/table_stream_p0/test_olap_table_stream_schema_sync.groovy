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

// The stream schema is generated dynamically from the base table, so a base table
// schema change (ADD/DROP COLUMN) must be reflected by the stream automatically.
suite("test_olap_table_stream_schema_sync", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_olap_table_stream_schema_sync_db"
    sql "CREATE DATABASE test_olap_table_stream_schema_sync_db"
    sql "USE test_olap_table_stream_schema_sync_db"

    def baseTable = "schema_sync_base"
    def streamName = "schema_sync_stream"

    def delta_time = 1000
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { tableName, opTimeout ->
        for (int t = delta_time; t <= opTimeout; t += delta_time) {
            def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if (alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(tableName + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= opTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    try {
        sql "DROP STREAM IF EXISTS ${streamName}"
        sql "DROP TABLE IF EXISTS ${baseTable}"

        sql """
            CREATE TABLE ${baseTable} (
                id BIGINT,
                v1 INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        sql "INSERT INTO ${baseTable} VALUES (1, 10)"
        sql """
            CREATE STREAM ${streamName}
            ON TABLE ${baseTable}
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "sync"

        // Initial schema: base visible columns (id, v1) + stream hidden columns.
        qt_desc_before "DESC ${streamName}"
        qt_show_columns_before "SHOW COLUMNS FROM ${streamName}"
        qt_info_schema_before """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE TABLE_SCHEMA = 'test_olap_table_stream_schema_sync_db'
              AND TABLE_NAME = '${streamName}'
            ORDER BY COLUMN_NAME
        """

        // ADD COLUMN on base table, the stream should expose the new column automatically.
        sql "ALTER TABLE ${baseTable} ADD COLUMN v2 VARCHAR(32) DEFAULT 'x'"
        wait_for_latest_op_on_table_finish(baseTable, 60000)
        sql "sync"
        qt_desc_after_add "DESC ${streamName}"
        qt_show_columns_after_add "SHOW COLUMNS FROM ${streamName}"
        qt_info_schema_after_add """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE TABLE_SCHEMA = 'test_olap_table_stream_schema_sync_db'
              AND TABLE_NAME = '${streamName}'
            ORDER BY COLUMN_NAME
        """

        sql "INSERT INTO ${baseTable} VALUES (1, 11, 'a')"
        sql "sync"
        sleep(1200)
        // SELECT * must return all current visible columns of the base table (id, v1, v2).
        order_qt_select_after_add """
            SELECT id, v1, v2, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM ${streamName}
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """

        // DROP COLUMN on base table, the stream should drop the column automatically.
        sql "ALTER TABLE ${baseTable} DROP COLUMN v2"
        wait_for_latest_op_on_table_finish(baseTable, 60000)
        sql "sync"
        qt_desc_after_drop "DESC ${streamName}"
        qt_show_columns_after_drop "SHOW COLUMNS FROM ${streamName}"
        qt_info_schema_after_drop """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE TABLE_SCHEMA = 'test_olap_table_stream_schema_sync_db'
              AND TABLE_NAME = '${streamName}'
            ORDER BY COLUMN_NAME
        """
    } finally {
        sql "DROP DATABASE IF EXISTS test_olap_table_stream_schema_sync_db"
    }
}
