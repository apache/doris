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

suite("test_row_ttl_vertical_compaction", "nonConcurrent") {
    setBeConfigTemporary([
        enable_vertical_compaction: true,
        vertical_compaction_num_columns_per_group: 3
    ]) {
        sql "DROP TABLE IF EXISTS row_ttl_vertical_dup"
        sql """
        CREATE TABLE row_ttl_vertical_dup (
            k INT,
            event_time DATETIMEV2(6) NULL,
            v1 INT, v2 INT, v3 INT, v4 INT, v5 INT, v6 INT,
            v7 INT, v8 INT, v9 INT, v10 INT, v11 INT, v12 INT
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "0"
        )
        """
        sql """
        INSERT INTO row_ttl_vertical_dup VALUES
            (1, now(6) - INTERVAL 1 DAY, 1,1,1,1,1,1,1,1,1,1,1,1)
        """
        sql """
        INSERT INTO row_ttl_vertical_dup VALUES
            (2, now(6) + INTERVAL 1 DAY, 2,2,2,2,2,2,2,2,2,2,2,2),
            (3, NULL, 3,3,3,3,3,3,3,3,3,3,3,3)
        """
        trigger_and_wait_compaction("row_ttl_vertical_dup", "full")
        order_qt_vertical_dup """
            SELECT k, v1, v6, v12 FROM row_ttl_vertical_dup ORDER BY k
        """

        sql "DROP TABLE IF EXISTS row_ttl_vertical_mow"
        sql """
        CREATE TABLE row_ttl_vertical_mow (
            k INT,
            event_time DATETIMEV2(6) NULL,
            v1 INT, v2 INT, v3 INT, v4 INT, v5 INT, v6 INT,
            v7 INT, v8 INT, v9 INT, v10 INT, v11 INT, v12 INT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "0"
        )
        """
        sql """
        INSERT INTO row_ttl_vertical_mow VALUES
            (1, now(6) + INTERVAL 1 DAY, 1,1,1,1,1,1,1,1,1,1,1,1),
            (2, now(6) + INTERVAL 1 DAY, 2,2,2,2,2,2,2,2,2,2,2,2)
        """
        sql """
        INSERT INTO row_ttl_vertical_mow VALUES
            (1, now(6) - INTERVAL 1 DAY, 9,9,9,9,9,9,9,9,9,9,9,9)
        """
        trigger_and_wait_compaction("row_ttl_vertical_mow", "full")
        order_qt_vertical_mow """
            SELECT k, v1, v6, v12 FROM row_ttl_vertical_mow ORDER BY k
        """
    }
}
