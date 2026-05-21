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

suite("test_row_binlog_basic", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_dup_with_binlog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_with_binlog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_with_before_binlog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_seq_with_binlog FORCE"

    sql """
        CREATE TABLE test_dup_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        DUPLICATE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_tso" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_tso" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_with_before_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_tso" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_seq_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_tso" = "true"
        )
    """

    sql """ALTER TABLE test_mow_seq_with_binlog
             ENABLE FEATURE "SEQUENCE_LOAD"
             WITH PROPERTIES ("function_column.sequence_type" = "int")"""

    sql """
        INSERT INTO test_dup_with_binlog VALUES
            (1, 1, 1, 10, '10'),
            (2, 2, 2, 20, '20')
    """
    sql """
        INSERT INTO test_dup_with_binlog VALUES
            (1, 1, 1, 11, '11'),
            (3, 3, 3, 30, '30')
    """

    order_qt_dup_raw """
        SELECT k1, k2, k3, v1, v2
        FROM test_dup_with_binlog
    """

    qt_dup_binlog """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2
        FROM binlog("table" = "test_dup_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    sql """
        INSERT INTO test_mow_with_binlog VALUES
            (1, 1, 1, 10, '10'),
            (2, 2, 2, 20, '20')
    """
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_mow_with_binlog(k1, k2, k3, v2) VALUES (2, 2, 2, '200')"
    sql "INSERT INTO test_mow_with_binlog(k1, k2, k3, v1) VALUES (2, 2, 2, 2000)"
    sql "INSERT INTO test_mow_with_binlog(k1, k2, k3, v2) VALUES (2, 2, 2, '201')"
    sql "SET enable_unique_key_partial_update = false"
    sql "DELETE FROM test_mow_with_binlog WHERE k1 = 1 AND k2 = 1 AND k3 = 1"

    order_qt_mow_raw """
        SELECT k1, k2, k3, v1, v2
        FROM test_mow_with_binlog
    """

    qt_mow_binlog """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2
        FROM binlog("table" = "test_mow_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    sql """
        INSERT INTO test_mow_with_before_binlog VALUES
            (1, 1, 1, 10, '10'),
            (2, 2, 2, 20, '20')
    """
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_mow_with_before_binlog(k1, k2, k3, v2) VALUES (2, 2, 2, '200')"
    sql "INSERT INTO test_mow_with_before_binlog(k1, k2, k3, v1) VALUES (2, 2, 2, 2000)"
    sql "INSERT INTO test_mow_with_before_binlog(k1, k2, k3, v2) VALUES (2, 2, 2, '201')"
    sql "SET enable_unique_key_partial_update = false"
    sql "DELETE FROM test_mow_with_before_binlog WHERE k1 = 1 AND k2 = 1 AND k3 = 1"

    order_qt_mow_before_raw """
        SELECT k1, k2, k3, v1, v2
        FROM test_mow_with_before_binlog
    """

    qt_mow_before_binlog """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               __BEFORE__v1__,
               __BEFORE__v2__
        FROM binlog("table" = "test_mow_with_before_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    sql """
        INSERT INTO test_mow_seq_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, 100, '100', 2)
    """
    sql """
        INSERT INTO test_mow_seq_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, 200, '200', 1)
    """
    sql """
        INSERT INTO test_mow_seq_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, 300, '300', 3)
    """
    sql "SET require_sequence_in_insert = false"
    sql "SET enable_unique_key_partial_update = true"
    sql """
        INSERT INTO test_mow_seq_with_binlog(k1, k2, k3, v2)
        VALUES (1, 1, 1, '350')
    """
    sql """
        INSERT INTO test_mow_seq_with_binlog(k1, k2, k3, v1, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, 400, 4)
    """
    sql """
        INSERT INTO test_mow_seq_with_binlog(k1, k2, k3, v2, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, '360', 2)
    """
    sql "SET enable_unique_key_partial_update = false"
    sql "SET require_sequence_in_insert = true"

    sql "DELETE FROM test_mow_seq_with_binlog WHERE k1 = 1 AND k2 = 1 AND k3 = 1"

    order_qt_mow_seq_raw """
        SELECT k1, k2, k3, v1, v2
        FROM test_mow_seq_with_binlog
    """

    sql "SET skip_delete_bitmap = false"

    qt_mow_seq_binlog """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               __BEFORE__v1__,
               __BEFORE__v2__
        FROM binlog("table" = "test_mow_seq_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    sql "SET skip_delete_bitmap = true"

    qt_mow_seq_binlog_skip_delete_bitmap """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               __BEFORE__v1__,
               __BEFORE__v2__
        FROM binlog("table" = "test_mow_seq_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """
    
    sql "SET skip_delete_bitmap = false"
}
