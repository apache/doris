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

suite("test_temporary_partition_row_count") {
    sql "DROP TABLE IF EXISTS test_temporary_partition_row_count"
    sql """CREATE TABLE test_temporary_partition_row_count (
            k1 INT NOT NULL,
            k2 INT
        ) DUPLICATE KEY(k1)
        PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN (10))
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ALTER TABLE test_temporary_partition_row_count
        ADD TEMPORARY PARTITION tp1 VALUES LESS THAN (10)"""
    sql """ALTER TABLE test_temporary_partition_row_count
        ADD TEMPORARY PARTITION tp2 VALUES LESS THAN (20)"""
    sql "INSERT INTO test_temporary_partition_row_count VALUES (1, 1), (2, 2), (3, 3)"
    sql """INSERT INTO test_temporary_partition_row_count TEMPORARY PARTITION (tp1)
        VALUES (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)"""
    sql """INSERT INTO test_temporary_partition_row_count TEMPORARY PARTITION (tp2)
        VALUES (11, 11), (12, 12)"""

    // Wait for both populations to be reported so the estimates do not depend on report timing.
    awaitUntil(180) {
        def normal = sql_return_maparray("SHOW PARTITIONS FROM test_temporary_partition_row_count")
        def temporary = sql_return_maparray("SHOW TEMPORARY PARTITIONS FROM test_temporary_partition_row_count")
        normal.find { it.PartitionName == "p1" }?.RowCount == "3" &&
            temporary.find { it.PartitionName == "tp1" }?.RowCount == "5" &&
            temporary.find { it.PartitionName == "tp2" }?.RowCount == "2"
    }

    explain {
        sql "SELECT * FROM test_temporary_partition_row_count"
        contains "cardinality=3, "
    }
    explain {
        sql "SELECT * FROM test_temporary_partition_row_count TEMPORARY PARTITION (tp1)"
        contains "cardinality=5, "
    }
    explain {
        sql "SELECT * FROM test_temporary_partition_row_count TEMPORARY PARTITION (tp2)"
        contains "cardinality=2, "
    }
    explain {
        sql "SELECT * FROM test_temporary_partition_row_count TEMPORARY PARTITION (tp1, tp2)"
        contains "cardinality=7, "
    }

    order_qt_normal "SELECT * FROM test_temporary_partition_row_count"
    order_qt_tp1 "SELECT * FROM test_temporary_partition_row_count TEMPORARY PARTITION (tp1)"
    order_qt_tp2 "SELECT * FROM test_temporary_partition_row_count TEMPORARY PARTITION (tp2)"
    order_qt_temporary "SELECT * FROM test_temporary_partition_row_count TEMPORARY PARTITION (tp1, tp2)"
}
