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

suite("test_binlog_compaction", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    setBeConfigTemporary([
        enable_vertical_compaction: true,
        enable_ordered_data_compaction: true,
        vertical_compaction_num_columns_per_group: 3,
        binlog_compaction_goal_size_mbytes: 0,
        binlog_compaction_file_count_threshold: 2,
        binlog_compaction_wait_timesec_after_visible: 0,
        binlog_compaction_time_threshold_seconds: 86400
    ]) {
        // Case 1: DUP_KEYS inserts.
        sql "DROP TABLE IF EXISTS test_binlog_compaction_dup FORCE"
        sql """
            CREATE TABLE test_binlog_compaction_dup (
                k1 INT,
                v1 INT,
                v2 STRING
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "enable_tso" = "true"
            )
        """

        sql """
            INSERT INTO test_binlog_compaction_dup VALUES
                (1, 10, 'a'),
                (2, 20, 'b')
        """
        sql """
            INSERT INTO test_binlog_compaction_dup VALUES
                (1, 11, 'a1'),
                (3, 30, 'c')
        """

        // Round 1: Level0 [0-1], [2-2], [3-3] -> Level1 [0-3].
        trigger_and_wait_compaction("test_binlog_compaction_dup", "binlog")

        qt_dup_binlog_compaction """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   v1,
                   v2
            FROM binlog("table" = "test_binlog_compaction_dup")
        """

        // Case 2: MOW inserts, historical values, and delete.
        sql "DROP TABLE IF EXISTS test_binlog_compaction_mow_historical FORCE"
        sql """
            CREATE TABLE test_binlog_compaction_mow_historical (
                k1 INT,
                v1 INT,
                v2 STRING
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true",
                "enable_tso" = "true"
            )
        """

        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (1, 10, 'a'),
                (2, 20, 'b')
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (1, 11, 'a1')
        """
        // Round 1: Level0 [0-1], [2-2], [3-3] -> Level1 [0-3].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")

        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (2, 21, 'b1')
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (3, 30, 'c')
        """
        // Round 2: Level0 [4-4], [5-5] -> Level1 [4-5].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")
        // Round 3: Level1 [0-3], [4-5] -> Level2 [0-5].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")

        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (1, 12, 'a2')
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (1, 13, 'a3')
        """
        // Round 4: Level0 [6-6], [7-7] -> Level1 [6-7].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")

        sql """
            INSERT INTO test_binlog_compaction_mow_historical VALUES
                (2, 22, 'b2')
        """
        sql "DELETE FROM test_binlog_compaction_mow_historical WHERE k1 = 3"
        // Round 5: Level0 [8-8], [9-9] -> Level1 [8-9].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")
        // Round 6: Level1 [6-7], [8-9] -> Level2 [6-9].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")
        // Round 7: Level2 [0-5], [6-9] -> [0-9].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical", "binlog")

        qt_mow_historical_binlog_compaction """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_binlog_compaction_mow_historical")
        """

        // Case 3: MOW sequence inserts, historical values, and delete.
        sql "DROP TABLE IF EXISTS test_binlog_compaction_mow_seq_historical FORCE"
        sql """
            CREATE TABLE test_binlog_compaction_mow_seq_historical (
                k1 INT,
                v1 INT,
                v2 STRING
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true",
                "enable_tso" = "true"
            )
        """

        sql """ALTER TABLE test_binlog_compaction_mow_seq_historical
                 ENABLE FEATURE "SEQUENCE_LOAD"
                 WITH PROPERTIES ("function_column.sequence_type" = "int")"""

        sql """
            INSERT INTO test_binlog_compaction_mow_seq_historical(k1, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES
                (1, 100, 'a1', 2),
                (2, 200, 'b1', 1)
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_seq_historical(k1, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES
                (1, 90, 'a0', 1)
        """
        // Round 1: Level0 [0-1], [2-2], [3-3] -> Level1 [0-3].
        trigger_and_wait_compaction("test_binlog_compaction_mow_seq_historical", "binlog")

        sql """
            INSERT INTO test_binlog_compaction_mow_seq_historical(k1, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES
                (1, 300, 'a3', 3)
        """
        sql "DELETE FROM test_binlog_compaction_mow_seq_historical WHERE k1 = 2"
        // Round 2: Level0 [4-4], [5-5] -> Level1 [4-5].
        trigger_and_wait_compaction("test_binlog_compaction_mow_seq_historical", "binlog")
        // Round 3: Level1 [0-3], [4-5] -> Level2 [0-5].
        trigger_and_wait_compaction("test_binlog_compaction_mow_seq_historical", "binlog")

        qt_mow_seq_historical_binlog_compaction """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_binlog_compaction_mow_seq_historical")
        """

        // Case 4: MOW long-chain inserts, historical values, and delete.
        sql "DROP TABLE IF EXISTS test_binlog_compaction_mow_historical_long_chain FORCE"
        sql """
            CREATE TABLE test_binlog_compaction_mow_historical_long_chain (
                k1 INT,
                v1 INT,
                v2 STRING
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true",
                "enable_tso" = "true"
            )
        """

        sql """
            INSERT INTO test_binlog_compaction_mow_historical_long_chain VALUES
                (1, 100, 'a0'),
                (2, 200, 'b0')
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_historical_long_chain VALUES
                (1, 110, 'a1')
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_historical_long_chain VALUES
                (1, 120, 'a2')
        """
        // Round 1: Level0 [0-1], [2-2], [3-3], [4-4] -> Level1 [0-4].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical_long_chain", "binlog")

        sql """
            INSERT INTO test_binlog_compaction_mow_historical_long_chain VALUES
                (1, 130, 'a3')
        """
        sql """
            INSERT INTO test_binlog_compaction_mow_historical_long_chain VALUES
                (1, 140, 'a4')
        """
        sql "DELETE FROM test_binlog_compaction_mow_historical_long_chain WHERE k1 = 2"
        // Round 2: Level0 [5-5], [6-6], [7-7] -> Level1 [5-7].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical_long_chain", "binlog")
        // Round 3: Level1 [0-4], [5-7] -> Level2 [0-7].
        trigger_and_wait_compaction("test_binlog_compaction_mow_historical_long_chain", "binlog")

        qt_mow_historical_long_chain_binlog_compaction """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_binlog_compaction_mow_historical_long_chain")
        """
    }
}
