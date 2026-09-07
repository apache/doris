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

suite("test_ivm_cross_db_single_base") {
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_cross_db_mv"""
    sql """DROP DATABASE IF EXISTS ivm_cross_db_src FORCE"""

    sql """CREATE DATABASE ivm_cross_db_src"""

    sql """
        CREATE TABLE ivm_cross_db_src.ivm_cross_db_base (
            id BIGINT NOT NULL,
            value BIGINT
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_cross_db_src.ivm_cross_db_base VALUES (1, 10), (2, 20)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_cross_db_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, value FROM ivm_cross_db_src.ivm_cross_db_base
    """

    order_qt_stream_location """
        SELECT DB_NAME, BASE_TABLE_DB, BASE_TABLE_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = 'regression_test_mtmv_p0_ivm'
          AND BASE_TABLE_NAME = 'ivm_cross_db_base'
    """

    sql """REFRESH MATERIALIZED VIEW ivm_cross_db_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_cross_db_mv")

    order_qt_mv_data_after_first_incremental """
        SELECT id, value FROM ivm_cross_db_mv
    """

    sql """INSERT INTO ivm_cross_db_src.ivm_cross_db_base VALUES (3, 30)"""
    sql """REFRESH MATERIALIZED VIEW ivm_cross_db_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_cross_db_mv")

    order_qt_mv_data_after_complete """
        SELECT id, value FROM ivm_cross_db_mv
    """

    sql """INSERT INTO ivm_cross_db_src.ivm_cross_db_base VALUES (4, 40)"""
    sql """REFRESH MATERIALIZED VIEW ivm_cross_db_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_cross_db_mv")

    order_qt_mv_data_after_incremental """
        SELECT id, value FROM ivm_cross_db_mv
    """
}
