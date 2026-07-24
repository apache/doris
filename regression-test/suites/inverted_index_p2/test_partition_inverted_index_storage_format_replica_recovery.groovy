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

suite("test_partition_inverted_index_storage_format_replica_recovery", "p2,nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def backends = sql "SHOW BACKENDS"
    if (backends.size() < 2) {
        return
    }

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

    def config = [
        disable_balance : true,
        schedule_slot_num_per_ssd_path : 1000,
        schedule_slot_num_per_hdd_path : 1000,
        schedule_batch_size: 1000,
    ]

    setFeConfigTemporary(config) {
        sql "DROP TABLE IF EXISTS test_partition_inverted_index_storage_format_replica_recovery FORCE"
        sql """
            CREATE TABLE test_partition_inverted_index_storage_format_replica_recovery (
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
        sql """
            INSERT INTO test_partition_inverted_index_storage_format_replica_recovery VALUES
                ("2023-01-01", "recovery token")
        """
        sql """ALTER TABLE test_partition_inverted_index_storage_format_replica_recovery
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_partition_inverted_index_storage_format_replica_recovery
                ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
        assertPartitionFormat(
                "test_partition_inverted_index_storage_format_replica_recovery", "p_v2", "V2")
        assertPartitionFormat(
                "test_partition_inverted_index_storage_format_replica_recovery", "p_v3", "V3")

        def oldTablets = sql_return_maparray("""
                SHOW TABLETS FROM test_partition_inverted_index_storage_format_replica_recovery PARTITION(p_v2)""")
        assertEquals(2, oldTablets.size())
        def oldTablet = oldTablets[0]
        sql """
            ADMIN SET REPLICA STATUS PROPERTIES(
                'tablet_id' = '${oldTablet.TabletId}',
                'backend_id' = '${oldTablet.BackendId}',
                'status' = 'drop'
            )
        """

        def recovered = false
        for (def retry = 0; retry < 300; retry++) {
            sql "SELECT * FROM test_partition_inverted_index_storage_format_replica_recovery"
            def tablets = sql_return_maparray("""
                    SHOW TABLETS FROM test_partition_inverted_index_storage_format_replica_recovery PARTITION(p_v2)""")
            def newReplica = tablets.find {
                it.TabletId == oldTablet.TabletId && it.BackendId != oldTablet.BackendId
            }
            if (newReplica != null) {
                recovered = true
                break
            }
            sleep(1000)
        }
        assertTrue(recovered)
        assertPartitionFormat(
                "test_partition_inverted_index_storage_format_replica_recovery", "p_v2", "V2")
        assertPartitionFormat(
                "test_partition_inverted_index_storage_format_replica_recovery", "p_v3", "V3")
    }
}
