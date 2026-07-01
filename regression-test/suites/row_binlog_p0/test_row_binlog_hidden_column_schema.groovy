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

suite("test_row_binlog_hidden_column_schema", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_mow_seq_hidden_column_row_binlog FORCE"

    sql """
        CREATE TABLE test_mow_seq_hidden_column_row_binlog (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """ALTER TABLE test_mow_seq_hidden_column_row_binlog
             ENABLE FEATURE "SEQUENCE_LOAD"
             WITH PROPERTIES ("function_column.sequence_type" = "int")"""

    sql """
        INSERT INTO test_mow_seq_hidden_column_row_binlog(k1, v1, __DORIS_SEQUENCE_COL__)
        VALUES (1, 10, 1)
    """
    sql """
        INSERT INTO test_mow_seq_hidden_column_row_binlog(k1, v1, __DORIS_SEQUENCE_COL__)
        VALUES (1, 20, 2)
    """
    sql "sync"

    qt_row_binlog_hidden_non_key_schema """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               v1,
               __BEFORE__v1__
        FROM binlog("table" = "test_mow_seq_hidden_column_row_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    test {
        sql """
            SELECT __DORIS_SEQUENCE_COL__
            FROM binlog("table" = "test_mow_seq_hidden_column_row_binlog")
        """
        exception "Unknown column"
    }
}
