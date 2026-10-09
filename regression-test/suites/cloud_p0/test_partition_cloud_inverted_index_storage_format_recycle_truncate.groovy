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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.Http

suite("test_partition_cloud_inverted_index_storage_format_recycle_truncate", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "true")

        def assertPartitionFormat = { tableName, partitionName, expectedFormat ->
            def partitions = sql_return_maparray(
                    "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'")
            assertEquals(1, partitions.size())
            assertEquals(expectedFormat, partitions[0].InvertedIndexStorageFormat)
            def partitionDetails = sql_return_maparray("SHOW PARTITION ${partitions[0].PartitionId}")
            assertEquals(1, partitionDetails.size())
            assertEquals(expectedFormat, partitionDetails[0].InvertedIndexStorageFormat)

            def tablets = sql_return_maparray(
                    "SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
            assertTrue(!tablets.isEmpty())
            def tabletMetas = tablets.collect { tablet ->
                Http.GET(tablet.MetaUrl, true, false)
            }
            def baseTabletMetas = tabletMetas.findAll { tabletMeta ->
                tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
            def rollupTabletMetas = tabletMetas.findAll { tabletMeta ->
                !tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
            assertEquals(1, baseTabletMetas.size())
            assertEquals(1, rollupTabletMetas.size())
            baseTabletMetas.each { tabletMeta ->
                assertEquals(expectedFormat, tabletMeta.inverted_index_storage_format)
            }
            rollupTabletMetas.each { tabletMeta ->
                assertTrue(!tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" })
            }
        }

        def getTabletIds = { tableName, partitionName ->
            def tablets = sql_return_maparray(
                    "SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
            assertTrue(!tablets.isEmpty())
            def baseTablets = tablets.findAll { tablet ->
                def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
                tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
            assertEquals(1, baseTablets.size())
            baseTablets.collect { it.TabletId }
        }

        def assertMixedPartitionFormats = { tableName ->
            assertPartitionFormat(tableName, "p_v2", "V2")
            assertPartitionFormat(tableName, "p_v3", "V3")
        }

        sql "DROP TABLE IF EXISTS test_cloud_partition_inv_idx_fmt_recycle_trunc"
        sql """
            CREATE TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc (
                k DATE NOT NULL,
                v VARCHAR(100) NULL,
                w INT NULL,
                INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            PARTITION BY RANGE(k) (
                PARTITION p_v2 VALUES LESS THAN ("2024-01-01")
            )
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            )
        """
        sql """ALTER TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc
                ADD ROLLUP r_v(v, w)"""
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE ROLLUP
                    WHERE TableName = 'test_cloud_partition_inv_idx_fmt_recycle_trunc'
                    ORDER BY CreateTime DESC LIMIT 1"""
            time 600
        }
        assertPartitionFormat("test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v2", "V2")

        sql """ALTER TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc
                ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
        sql """
            INSERT INTO test_cloud_partition_inv_idx_fmt_recycle_trunc VALUES
                ("2023-01-01", "v2 recycle token", 1),
                ("2024-01-01", "v3 recycle token", 2)
        """
        assertMixedPartitionFormats("test_cloud_partition_inv_idx_fmt_recycle_trunc")

        sql """ALTER TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc
                DROP PARTITION p_v2"""
        sql """RECOVER PARTITION p_v2 FROM test_cloud_partition_inv_idx_fmt_recycle_trunc"""
        assertMixedPartitionFormats("test_cloud_partition_inv_idx_fmt_recycle_trunc")

        sql "DROP TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc"
        sql "RECOVER TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc"
        assertMixedPartitionFormats("test_cloud_partition_inv_idx_fmt_recycle_trunc")

        def v2TabletIdsBeforeTruncate = getTabletIds(
                "test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v2")
        def v3TabletIdsBeforeTruncate = getTabletIds(
                "test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v3")
        sql "TRUNCATE TABLE test_cloud_partition_inv_idx_fmt_recycle_trunc"
        def v2TabletIdsAfterTruncate = getTabletIds(
                "test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v2")
        def v3TabletIdsAfterTruncate = getTabletIds(
                "test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v3")
        assertEquals(v2TabletIdsBeforeTruncate.size(), v2TabletIdsAfterTruncate.size())
        assertEquals(v3TabletIdsBeforeTruncate.size(), v3TabletIdsAfterTruncate.size())
        assertTrue(v2TabletIdsBeforeTruncate.every { tabletId -> !v2TabletIdsAfterTruncate.contains(tabletId) })
        assertTrue(v3TabletIdsBeforeTruncate.every { tabletId -> !v3TabletIdsAfterTruncate.contains(tabletId) })
        assertPartitionFormat("test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v2", "V3")
        assertPartitionFormat("test_cloud_partition_inv_idx_fmt_recycle_trunc", "p_v3", "V3")

        sql """
            INSERT INTO test_cloud_partition_inv_idx_fmt_recycle_trunc VALUES
                ("2023-01-02", "v2 truncate token", 1),
                ("2024-01-02", "v3 truncate token", 2)
        """
        def rows = sql """
            SELECT CAST(k AS STRING), v
        FROM test_cloud_partition_inv_idx_fmt_recycle_trunc
            WHERE v MATCH_ANY "truncate" ORDER BY k
        """
        assertEquals([[
            "2023-01-02", "v2 truncate token"
        ], [
            "2024-01-02", "v3 truncate token"
        ]], rows)
    }
}
