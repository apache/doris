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

suite("test_row_binlog_partial_update_only_keys", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_row_binlog_partial_update_only_keys FORCE"

    sql """
        CREATE TABLE test_row_binlog_partial_update_only_keys (
            k1 INT,
            k2 INT,
            v_default INT NOT NULL DEFAULT "7",
            v_nullable STRING NULL
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO test_row_binlog_partial_update_only_keys VALUES
            (1, 10, 100, 'old-1'),
            (2, 20, 200, 'old-2')
    """

    sql "SET enable_unique_key_partial_update = true"
    sql "SET partial_update_new_key_behavior = 'APPEND'"
    sql """
        INSERT INTO test_row_binlog_partial_update_only_keys(k1, k2) VALUES
            (1, 10),
            (3, 30)
    """

    def queryTable = {
        sql """
            SELECT k1, k2, v_default, v_nullable
            FROM test_row_binlog_partial_update_only_keys
            ORDER BY k1, k2
        """
    }
    def queryBinlog = {
        sql """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   v_default,
                   v_nullable,
                   __DORIS_BEFORE__v_default__,
                   __DORIS_BEFORE__v_nullable__
            FROM binlog("table" = "test_row_binlog_partial_update_only_keys")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """
    }

    def expectedTable = [
        [1, 10, 100, "old-1"],
        [2, 20, 200, "old-2"],
        [3, 30, 7, null]
    ]
    assertEquals(expectedTable, queryTable())

    def expectedAppendBinlog = [
        [0L, 1, 10, 100, "old-1", null, null],
        [0L, 2, 20, 200, "old-2", null, null],
        [1L, 1, 10, 100, "old-1", 100, "old-1"],
        [0L, 3, 30, 7, null, null, null]
    ]
    assertEquals(expectedAppendBinlog, queryBinlog())

    sql "SET partial_update_new_key_behavior = 'ERROR'"
    sql """
        INSERT INTO test_row_binlog_partial_update_only_keys(k1, k2) VALUES
            (1, 10),
            (2, 20)
    """

    assertEquals(expectedTable, queryTable())
    def expectedErrorExistingBinlog = expectedAppendBinlog + [
        [1L, 1, 10, 100, "old-1", 100, "old-1"],
        [1L, 2, 20, 200, "old-2", 200, "old-2"]
    ]
    assertEquals(expectedErrorExistingBinlog, queryBinlog())

    test {
        sql """
            INSERT INTO test_row_binlog_partial_update_only_keys(k1, k2) VALUES
                (1, 10),
                (4, 40)
        """
        exception "[E-7003]Can't append new rows in partial update when partial_update_new_key_behavior is ERROR"
    }

    assertEquals(expectedTable, queryTable())
    assertEquals(expectedErrorExistingBinlog, queryBinlog())
}
