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

suite("test_ivm_replace_stream_cleanup") {
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_replace_old_mv"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_replace_new_mv"
    sql "DROP TABLE IF EXISTS ivm_replace_old_base"
    sql "DROP TABLE IF EXISTS ivm_replace_new_base"

    sql """
        CREATE TABLE ivm_replace_old_base (
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
        CREATE TABLE ivm_replace_new_base (
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
        CREATE MATERIALIZED VIEW ivm_replace_old_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_replace_old_base
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_replace_new_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_replace_new_base
    """

    sql "INSERT INTO ivm_replace_old_base VALUES (1, 10), (2, 20)"
    sql "REFRESH MATERIALIZED VIEW ivm_replace_old_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_replace_old_mv")
    order_qt_old_mv_before_replace "SELECT k1, v1 FROM ivm_replace_old_mv ORDER BY k1"

    sql "INSERT INTO ivm_replace_new_base VALUES (10, 100)"
    sql "REFRESH MATERIALIZED VIEW ivm_replace_new_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_replace_new_mv")
    order_qt_new_mv_before_replace "SELECT k1, v1 FROM ivm_replace_new_mv ORDER BY k1"

    qt_streams_before_replace """
        SELECT
            COUNT(DISTINCT CASE WHEN RIGHT(STREAM_NAME,
                    LENGTH('ivm_replace_old_base'))
                = 'ivm_replace_old_base' THEN STREAM_NAME END) AS old_streams,
            COUNT(DISTINCT CASE WHEN RIGHT(STREAM_NAME,
                    LENGTH('ivm_replace_new_base'))
                = 'ivm_replace_new_base' THEN STREAM_NAME END) AS new_streams
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = '${context.dbName}'
    """

    sql """
        ALTER MATERIALIZED VIEW ivm_replace_old_mv
        REPLACE WITH MATERIALIZED VIEW ivm_replace_new_mv
        PROPERTIES ('swap' = 'false')
    """

    qt_streams_after_replace """
        SELECT
            COUNT(DISTINCT CASE WHEN RIGHT(STREAM_NAME,
                    LENGTH('ivm_replace_old_base'))
                = 'ivm_replace_old_base' THEN STREAM_NAME END) AS old_streams,
            COUNT(DISTINCT CASE WHEN RIGHT(STREAM_NAME,
                    LENGTH('ivm_replace_new_base'))
                = 'ivm_replace_new_base' THEN STREAM_NAME END) AS new_streams
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = '${context.dbName}'
    """

    sql "INSERT INTO ivm_replace_new_base VALUES (20, 200)"
    sql "REFRESH MATERIALIZED VIEW ivm_replace_old_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_replace_old_mv")
    order_qt_mv_after_replace "SELECT k1, v1 FROM ivm_replace_old_mv ORDER BY k1"
}
