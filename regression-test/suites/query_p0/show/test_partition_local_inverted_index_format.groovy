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

suite("test_show_inverted_index_storage_format") {
    if (isCloudMode()) {
        return
    }

    def assertPartitionFormats = { tableName, expectedPartitionCount, expectedFormat ->
        def partitions = sql_return_maparray("SHOW PARTITIONS FROM ${tableName}")
        assertEquals(expectedPartitionCount, partitions.size())
        assertTrue(partitions.every { !it.containsKey("InvertedIndexStorageFormat") })
        def partitionDetails = partitions.collect {
            sql_return_maparray("SHOW PARTITION ${it.PartitionId}")
        }
        assertTrue(partitionDetails.every { it.size() == 1 })
        assertTrue(partitionDetails.every { it[0].InvertedIndexStorageFormat == expectedFormat })
    }

    sql "DROP TABLE IF EXISTS test_show_inverted_format_range"
    sql """
        CREATE TABLE test_show_inverted_format_range (
            k INT,
            v VARCHAR(20),
            INDEX idx_v (v) USING INVERTED
        )
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p1 VALUES [("0"), ("10")),
            PARTITION p2 VALUES [("10"), ("20")),
            PARTITION p3 VALUES [("20"), ("30"))
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V3"
        )
    """
    sql """
        INSERT INTO test_show_inverted_format_range VALUES
            (1, 'range-1a'), (2, 'range-1b'), (3, 'range-1c'),
            (11, 'range-2a'), (12, 'range-2b'), (13, 'range-2c'),
            (21, 'range-3a'), (22, 'range-3b'), (23, 'range-3c')
    """

    sql "DROP TABLE IF EXISTS test_show_inverted_format_list"
    sql """
        CREATE TABLE test_show_inverted_format_list (
            k INT,
            category VARCHAR(20),
            INDEX idx_category (category) USING INVERTED
        )
        DUPLICATE KEY(k, category)
        PARTITION BY LIST(category) (
            PARTITION p_east VALUES IN ('east', 'north'),
            PARTITION p_west VALUES IN ('west', 'south'),
            PARTITION p_central VALUES IN ('central', 'overseas')
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V2"
        )
    """
    sql """
        INSERT INTO test_show_inverted_format_list VALUES
            (1, 'east'), (2, 'north'), (3, 'west'),
            (4, 'south'), (5, 'central'), (6, 'overseas')
    """

    sql "DROP TABLE IF EXISTS test_show_inverted_format_auto"
    sql """
        CREATE TABLE test_show_inverted_format_auto (
            k INT,
            category VARCHAR(20),
            INDEX idx_category (category) USING INVERTED
        )
        DUPLICATE KEY(k, category)
        AUTO PARTITION BY LIST(category) ()
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V3"
        )
    """
    sql """
        INSERT INTO test_show_inverted_format_auto VALUES
            (1, 'book'), (2, 'book'), (3, 'music'),
            (4, 'music'), (5, 'video'), (6, 'video')
    """

    sql "DROP TABLE IF EXISTS test_show_inverted_format_dynamic"
    sql """
        CREATE TABLE test_show_inverted_format_dynamic (
            k INT,
            dt DATE,
            v VARCHAR(20),
            INDEX idx_v (v) USING INVERTED
        )
        DUPLICATE KEY(k, dt)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-3",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.create_history_partition" = "true",
            "inverted_index_storage_format" = "V3"
        )
    """
    sql """
        INSERT INTO test_show_inverted_format_dynamic
        SELECT 1, date_sub(curdate(), INTERVAL 2 DAY), 'dynamic-1a'
        UNION ALL SELECT 2, date_sub(curdate(), INTERVAL 2 DAY), 'dynamic-1b'
        UNION ALL SELECT 3, date_sub(curdate(), INTERVAL 1 DAY), 'dynamic-2a'
        UNION ALL SELECT 4, date_sub(curdate(), INTERVAL 1 DAY), 'dynamic-2b'
        UNION ALL SELECT 5, curdate(), 'dynamic-3a'
        UNION ALL SELECT 6, curdate(), 'dynamic-3b'
    """
    sql "SYNC"

    assertPartitionFormats("test_show_inverted_format_range", 3, "V3")
    assertPartitionFormats("test_show_inverted_format_list", 3, "V2")
    assertPartitionFormats("test_show_inverted_format_auto", 3, "V3")
    assertPartitionFormats("test_show_inverted_format_dynamic", 7, "V3")

}
