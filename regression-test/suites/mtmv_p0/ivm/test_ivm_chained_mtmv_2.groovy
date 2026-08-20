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

suite("test_ivm_chained_mtmv_2") {
    sql """DROP MATERIALIZED VIEW IF EXISTS child_ivm_commit_tso;"""
    sql """DROP MATERIALIZED VIEW IF EXISTS root_ivm_commit_tso;"""
    sql """DROP TABLE IF EXISTS base_tbl_commit_tso;"""

    sql """
        CREATE TABLE base_tbl_commit_tso (
            id BIGINT NOT NULL,
            grp INT NOT NULL,
            amount BIGINT NOT NULL,
            note VARCHAR(32)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """
        INSERT INTO base_tbl_commit_tso VALUES
            (1, 1, 10, 'one'),
            (2, 1, 20, 'two'),
            (3, 2, 30, 'three');
    """

    sql """
        CREATE MATERIALIZED VIEW root_ivm_commit_tso
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
        AS SELECT id, grp, amount, note FROM base_tbl_commit_tso;
    """

    sql """REFRESH MATERIALIZED VIEW root_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("root_ivm_commit_tso")
    order_qt_root_after_first_refresh """
        SELECT id, grp, amount, note FROM root_ivm_commit_tso ORDER BY id;
    """

    sql """
        CREATE MATERIALIZED VIEW child_ivm_commit_tso
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, amount FROM root_ivm_commit_tso;
    """

    def rootStreamName = (sql """
        SELECT STREAM_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_NAME = 'root_ivm_commit_tso'
        ORDER BY STREAM_NAME
        LIMIT 1;
    """)[0][0]

    order_qt_child_source_stream """
        SELECT * FROM ${rootStreamName} ORDER BY id;
    """

    sql """REFRESH MATERIALIZED VIEW child_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("child_ivm_commit_tso")
    order_qt_child_after_first_refresh """
        SELECT id, amount FROM child_ivm_commit_tso ORDER BY id;
    """

    sql """UPDATE base_tbl_commit_tso SET amount = 15, note = 'one-updated' WHERE id = 1;"""
    sql """DELETE FROM base_tbl_commit_tso WHERE id = 2;"""
    sql """INSERT INTO base_tbl_commit_tso VALUES (4, 2, 40, 'four');"""
    sql """REFRESH MATERIALIZED VIEW root_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("root_ivm_commit_tso")
    sql """REFRESH MATERIALIZED VIEW child_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("child_ivm_commit_tso")

    order_qt_root_after_second_refresh """
        SELECT id, grp, amount, note FROM root_ivm_commit_tso ORDER BY id;
    """
    order_qt_child_after_second_refresh """
        SELECT id, amount FROM child_ivm_commit_tso ORDER BY id;
    """

    sql """UPDATE base_tbl_commit_tso SET amount = 35 WHERE id = 3;"""
    sql """DELETE FROM base_tbl_commit_tso WHERE id = 1;"""
    sql """INSERT INTO base_tbl_commit_tso VALUES (5, 3, 50, 'five');"""
    sql """REFRESH MATERIALIZED VIEW root_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("root_ivm_commit_tso")
    sql """REFRESH MATERIALIZED VIEW child_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("child_ivm_commit_tso")

    order_qt_root_after_third_refresh """
        SELECT id, grp, amount, note FROM root_ivm_commit_tso ORDER BY id;
    """
    order_qt_child_after_third_refresh """
        SELECT id, amount FROM child_ivm_commit_tso ORDER BY id;
    """

    sql """REFRESH MATERIALIZED VIEW root_ivm_commit_tso COMPLETE;"""
    waitingMTMVTaskFinishedByMvName("root_ivm_commit_tso")

    sql """REFRESH MATERIALIZED VIEW child_ivm_commit_tso INCREMENTAL;"""
    waitingMTMVTaskFinishedNotNeedSuccess(getJobName(context.dbName, "child_ivm_commit_tso"))
    order_qt_child_incremental_after_root_complete """
        SELECT Status, ErrorMsg
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'child_ivm_commit_tso'
        ORDER BY CreateTime DESC LIMIT 1;
    """

    sql """REFRESH MATERIALIZED VIEW child_ivm_commit_tso AUTO;"""
    waitingMTMVTaskFinishedByMvName("child_ivm_commit_tso")
    order_qt_child_auto_refresh_mode """
        SELECT RefreshMode
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'child_ivm_commit_tso'
        ORDER BY CreateTime DESC LIMIT 1;
    """
    order_qt_child_after_auto_refresh """
        SELECT id, amount FROM child_ivm_commit_tso ORDER BY id;
    """
}
