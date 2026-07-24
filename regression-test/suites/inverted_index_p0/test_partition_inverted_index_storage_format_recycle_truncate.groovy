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

suite("test_partition_inverted_index_storage_format_recycle_truncate", "p0,nonConcurrent") {
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

    def getTabletIds = { tableName, partitionName ->
        def tablets = sql_return_maparray(
                "SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
        assertTrue(!tablets.isEmpty())
        tablets.collect { it.TabletId }
    }

    def assertMixedPartitionFormats = { tableName ->
        assertPartitionFormat(tableName, "p_v2", "V2")
        assertPartitionFormat(tableName, "p_v3", "V3")
    }

    sql "DROP TABLE IF EXISTS test_partition_inverted_index_storage_format_recycle_truncate"
    sql """
        CREATE TABLE test_partition_inverted_index_storage_format_recycle_truncate (
            k DATE NOT NULL,
            v VARCHAR(100) NULL,
            INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p_v2 VALUES LESS THAN ("2024-01-01")
        )
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "V2"
        )
    """
    sql """ALTER TABLE test_partition_inverted_index_storage_format_recycle_truncate
            SET ("partition.inverted_index_storage_format" = "V3")"""
    sql """ALTER TABLE test_partition_inverted_index_storage_format_recycle_truncate
            ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
    sql """
        INSERT INTO test_partition_inverted_index_storage_format_recycle_truncate VALUES
            ("2023-01-01", "v2 recycle token"),
            ("2024-01-01", "v3 recycle token")
    """
    assertMixedPartitionFormats("test_partition_inverted_index_storage_format_recycle_truncate")

    sql """ALTER TABLE test_partition_inverted_index_storage_format_recycle_truncate
            DROP PARTITION p_v2"""
    sql """RECOVER PARTITION p_v2 FROM test_partition_inverted_index_storage_format_recycle_truncate"""
    assertMixedPartitionFormats("test_partition_inverted_index_storage_format_recycle_truncate")

    sql "DROP TABLE test_partition_inverted_index_storage_format_recycle_truncate"
    sql "RECOVER TABLE test_partition_inverted_index_storage_format_recycle_truncate"
    assertMixedPartitionFormats("test_partition_inverted_index_storage_format_recycle_truncate")

    def v2TabletIdsBeforeTruncate = getTabletIds(
            "test_partition_inverted_index_storage_format_recycle_truncate", "p_v2")
    def v3TabletIdsBeforeTruncate = getTabletIds(
            "test_partition_inverted_index_storage_format_recycle_truncate", "p_v3")
    sql "TRUNCATE TABLE test_partition_inverted_index_storage_format_recycle_truncate"
    def v2TabletIdsAfterTruncate = getTabletIds(
            "test_partition_inverted_index_storage_format_recycle_truncate", "p_v2")
    def v3TabletIdsAfterTruncate = getTabletIds(
            "test_partition_inverted_index_storage_format_recycle_truncate", "p_v3")
    assertEquals(v2TabletIdsBeforeTruncate.size(), v2TabletIdsAfterTruncate.size())
    assertEquals(v3TabletIdsBeforeTruncate.size(), v3TabletIdsAfterTruncate.size())
    assertTrue(v2TabletIdsBeforeTruncate.every { tabletId -> !v2TabletIdsAfterTruncate.contains(tabletId) })
    assertTrue(v3TabletIdsBeforeTruncate.every { tabletId -> !v3TabletIdsAfterTruncate.contains(tabletId) })
    assertMixedPartitionFormats("test_partition_inverted_index_storage_format_recycle_truncate")

    sql """
        INSERT INTO test_partition_inverted_index_storage_format_recycle_truncate VALUES
            ("2023-01-02", "v2 truncate token"),
            ("2024-01-02", "v3 truncate token")
    """
    def rows = sql """
        SELECT CAST(k AS STRING), v
        FROM test_partition_inverted_index_storage_format_recycle_truncate
        WHERE v MATCH_ANY "truncate" ORDER BY k
    """
    assertEquals([[
        "2023-01-02", "v2 truncate token"
    ], [
        "2024-01-02", "v3 truncate token"
    ]], rows)
}
