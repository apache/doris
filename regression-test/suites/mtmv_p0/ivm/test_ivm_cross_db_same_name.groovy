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

suite("test_ivm_cross_db_same_name") {
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_cross_db_same_name_mv"
    sql "DROP DATABASE IF EXISTS ivm_same_name_left FORCE"
    sql "DROP DATABASE IF EXISTS ivm_same_name_right FORCE"

    sql "CREATE DATABASE ivm_same_name_left"
    sql "CREATE DATABASE ivm_same_name_right"

    sql """
        CREATE TABLE ivm_same_name_left.same_name (
            id BIGINT NOT NULL,
            value BIGINT
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW',
            'binlog.need_historical_value' = 'true'
        )
    """
    sql """
        CREATE TABLE ivm_same_name_right.same_name (
            id BIGINT NOT NULL,
            value BIGINT
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'binlog.enable' = 'true',
            'binlog.format' = 'ROW',
            'binlog.need_historical_value' = 'true'
        )
    """

    sql "INSERT INTO ivm_same_name_left.same_name VALUES (1, 10), (2, 20)"
    sql "INSERT INTO ivm_same_name_right.same_name VALUES (1, 100), (3, 300)"

    sql """
        CREATE MATERIALIZED VIEW ivm_cross_db_same_name_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT l.id, l.value AS left_value, r.value AS right_value
        FROM ivm_same_name_left.same_name l
        INNER JOIN ivm_same_name_right.same_name r ON l.id = r.id
    """

    def mvRows = sql """
        SELECT Id
        FROM mv_infos('database'='${context.dbName}')
        WHERE Name = 'ivm_cross_db_same_name_mv'
    """
    assertEquals(1, mvRows.size())
    def mvId = mvRows[0][0].toString()

    qt_stream_identity """
        SELECT COUNT(*) AS stream_count,
               COUNT(DISTINCT STREAM_NAME) AS distinct_names,
               COUNT(DISTINCT STREAM_ID) AS distinct_ids,
               COUNT(DISTINCT BASE_TABLE_DB) AS distinct_base_dbs
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_NAME = 'same_name'
          AND BASE_TABLE_DB IN ('ivm_same_name_left', 'ivm_same_name_right')
          AND starts_with(STREAM_NAME, '__doris_ivm_stream_${mvId}_')
    """

    order_qt_stream_bases """
        SELECT BASE_TABLE_CTL, BASE_TABLE_DB, BASE_TABLE_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_NAME = 'same_name'
          AND BASE_TABLE_DB IN ('ivm_same_name_left', 'ivm_same_name_right')
          AND starts_with(STREAM_NAME, '__doris_ivm_stream_${mvId}_')
        ORDER BY BASE_TABLE_CTL, BASE_TABLE_DB, BASE_TABLE_NAME
    """

    sql "REFRESH MATERIALIZED VIEW ivm_cross_db_same_name_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_cross_db_same_name_mv")
    order_qt_initial_result """
        SELECT id, left_value, right_value
        FROM ivm_cross_db_same_name_mv
        ORDER BY id
    """

    sql "INSERT INTO ivm_same_name_left.same_name VALUES (3, 30)"
    sql "REFRESH MATERIALIZED VIEW ivm_cross_db_same_name_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_cross_db_same_name_mv")
    order_qt_after_left_delta """
        SELECT id, left_value, right_value
        FROM ivm_cross_db_same_name_mv
        ORDER BY id
    """

    sql "INSERT INTO ivm_same_name_right.same_name VALUES (2, 200)"
    sql "REFRESH MATERIALIZED VIEW ivm_cross_db_same_name_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_cross_db_same_name_mv")
    order_qt_after_right_delta """
        SELECT id, left_value, right_value
        FROM ivm_cross_db_same_name_mv
        ORDER BY id
    """
}
