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

import org.apache.doris.regression.util.Http

suite("test_partition_inverted_index_storage_format_rollout", "p0,nonConcurrent") {
    def assertPartitionFormat = { tableName, partitionName, expectedFormat ->
        def partitions = sql_return_maparray(
                "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'")
        assertEquals(1, partitions.size())
        def partitionDetails = sql_return_maparray("SHOW PARTITION ${partitions[0].PartitionId}")
        assertEquals(1, partitionDetails.size())
        assertEquals(expectedFormat, partitionDetails[0].InvertedIndexStorageFormat)

        def tablets = sql_return_maparray(
                "SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
        assertTrue(!tablets.isEmpty())
        tablets.each { tablet ->
            def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
            assertEquals(expectedFormat, tabletMeta.schema.inverted_index_storage_format)
        }
    }

    sql "DROP TABLE IF EXISTS test_partition_inverted_index_storage_format_rollout"
    sql "DROP TABLE IF EXISTS test_partition_inverted_index_storage_format_initial"
    sql "DROP TABLE IF EXISTS test_partition_inverted_index_storage_format_auto"
    sql "DROP TABLE IF EXISTS test_partition_inverted_index_storage_format_dynamic"

    sql """
        CREATE TABLE test_partition_inverted_index_storage_format_rollout (
            k DATE NOT NULL,
            v VARCHAR(100) NULL,
            INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p_old VALUES LESS THAN ("2024-01-01")
        )
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V2"
        )
    """

    sql """ALTER TABLE test_partition_inverted_index_storage_format_rollout ADD PARTITION p_default VALUES [("2026-01-01"), ("2027-01-01"))"""
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_rollout", "p_old", "V2")
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_rollout", "p_default", "V2")

    sql """ALTER TABLE test_partition_inverted_index_storage_format_rollout SET ("partition.inverted_index_storage_format" = "V3")"""
    sql """ALTER TABLE test_partition_inverted_index_storage_format_rollout ADD PARTITION p_new VALUES [("2024-01-01"), ("2025-01-01"))"""
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_rollout", "p_new", "V3")

    sql """ALTER TABLE test_partition_inverted_index_storage_format_rollout SET ("partition.inverted_index_storage_format" = "V2")"""
    sql """ALTER TABLE test_partition_inverted_index_storage_format_rollout ADD PARTITION p_downgrade VALUES [("2025-01-01"), ("2026-01-01"))"""
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_rollout", "p_downgrade", "V2")

    sql """
        INSERT INTO test_partition_inverted_index_storage_format_rollout VALUES
            ("2023-01-01", "old token"),
            ("2024-01-01", "new token"),
            ("2025-01-01", "downgrade token")
    """
    def mixedFormatRows = sql """
        SELECT CAST(k AS STRING), v
        FROM test_partition_inverted_index_storage_format_rollout
        WHERE v MATCH_ANY 'token' ORDER BY k
    """
    assertEquals([
        ["2023-01-01", "old token"],
        ["2024-01-01", "new token"],
        ["2025-01-01", "downgrade token"]
    ], mixedFormatRows)

    sql """ALTER TABLE test_partition_inverted_index_storage_format_rollout SET ("partition.inverted_index_storage_format" = "V3")"""
    sql """INSERT OVERWRITE TABLE test_partition_inverted_index_storage_format_rollout PARTITION(p_old) VALUES ("2023-01-02", "old rewritten")"""
    def rewrittenRows = sql """
        SELECT CAST(k AS STRING), v
        FROM test_partition_inverted_index_storage_format_rollout
        WHERE v MATCH_ANY 'rewritten' ORDER BY k
    """
    assertEquals([["2023-01-02", "old rewritten"]], rewrittenRows)
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_rollout", "p_old", "V2")

    sql """
        CREATE TABLE test_partition_inverted_index_storage_format_initial (
            k DATE NOT NULL,
            v VARCHAR(100) NULL,
            INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p_initial VALUES LESS THAN ("2024-01-01")
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V2",
            "partition.inverted_index_storage_format" = "V3"
        )
    """
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_initial", "p_initial", "V3")

    sql """
        CREATE TABLE test_partition_inverted_index_storage_format_auto (
            k VARCHAR(20) NOT NULL,
            v VARCHAR(100) NULL,
            INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        AUTO PARTITION BY LIST(k) ()
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V2"
        )
    """
    sql """INSERT INTO test_partition_inverted_index_storage_format_auto VALUES ("old", "auto old")"""
    def autoPartitions = sql_return_maparray(
            "SHOW PARTITIONS FROM test_partition_inverted_index_storage_format_auto")
    assertEquals(1, autoPartitions.size())
    def oldAutoPartitionName = autoPartitions[0].PartitionName
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_auto", oldAutoPartitionName, "V2")

    sql """ALTER TABLE test_partition_inverted_index_storage_format_auto SET ("partition.inverted_index_storage_format" = "V3")"""
    sql """INSERT INTO test_partition_inverted_index_storage_format_auto VALUES ("new", "auto new")"""
    autoPartitions = sql_return_maparray(
            "SHOW PARTITIONS FROM test_partition_inverted_index_storage_format_auto")
    assertEquals(2, autoPartitions.size())
    def newAutoPartition = autoPartitions.find { it.PartitionName != oldAutoPartitionName }
    assertNotNull(newAutoPartition)
    assertPartitionFormat(
            "test_partition_inverted_index_storage_format_auto", newAutoPartition.PartitionName, "V3")
    def autoRows = sql """
        SELECT k, v FROM test_partition_inverted_index_storage_format_auto
        WHERE v MATCH_ANY 'auto' ORDER BY k
    """
    assertEquals([["new", "auto new"], ["old", "auto old"]], autoRows)

    def oldDynamicPartitionCheckInterval = getFeConfig('dynamic_partition_check_interval_seconds')
    try {
        setFeConfig('dynamic_partition_check_interval_seconds', 1)
        sql """
            CREATE TABLE test_partition_inverted_index_storage_format_dynamic (
                k DATE NOT NULL,
                v VARCHAR(100) NULL,
                INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            PARTITION BY RANGE(k) ()
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-1",
                "dynamic_partition.end" = "1",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1",
                "dynamic_partition.create_history_partition" = "true",
                "inverted_index_storage_format" = "V2"
            )
        """
        def dynamicPartitions = sql_return_maparray(
                "SHOW PARTITIONS FROM test_partition_inverted_index_storage_format_dynamic")
        assertEquals(3, dynamicPartitions.size())
        def initialDynamicPartitionNames = dynamicPartitions.collect { it.PartitionName }
        initialDynamicPartitionNames.each { partitionName ->
            assertPartitionFormat(
                    "test_partition_inverted_index_storage_format_dynamic", partitionName, "V2")
        }

        sql """ALTER TABLE test_partition_inverted_index_storage_format_dynamic SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_partition_inverted_index_storage_format_dynamic SET ("dynamic_partition.end" = "3")"""
        for (def retry = 0; retry < 120; retry++) {
            dynamicPartitions = sql_return_maparray(
                    "SHOW PARTITIONS FROM test_partition_inverted_index_storage_format_dynamic")
            if (dynamicPartitions.size() == 5) {
                break
            }
            sleep(1000)
        }
        assertEquals(5, dynamicPartitions.size())
        dynamicPartitions.each { partition ->
            def expectedFormat = initialDynamicPartitionNames.contains(partition.PartitionName) ? "V2" : "V3"
            assertPartitionFormat(
                    "test_partition_inverted_index_storage_format_dynamic",
                    partition.PartitionName, expectedFormat)
        }
    } finally {
        setFeConfig('dynamic_partition_check_interval_seconds', oldDynamicPartitionCheckInterval)
    }
}
