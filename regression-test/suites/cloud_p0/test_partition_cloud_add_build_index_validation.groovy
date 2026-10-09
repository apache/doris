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

suite("test_partition_cloud_add_build_index_validation", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
        def assertPartitionFormat = { tableName, partitionName, expectedFormat ->
            def partitions = sql_return_maparray(
                    "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'")
            assertEquals(1, partitions.size())
            assertEquals(expectedFormat, partitions[0].InvertedIndexStorageFormat)
        }

        def waitForLatestSchemaChangeDone = { tableName ->
            waitForSchemaChangeDone {
                sql """SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}'
                        ORDER BY CreateTime DESC LIMIT 1"""
                time 600
            }
        }

        sql "DROP TABLE IF EXISTS test_cloud_add_index_variant_v2_reapplied"
        sql "DROP TABLE IF EXISTS test_cloud_partition_format_rollout_disabled"
        sql "DROP TABLE IF EXISTS test_cloud_dict_compression_v2"

        def disabledRolloutTable = "test_cloud_partition_format_rollout_disabled"
        sql """
            CREATE TABLE ${disabledRolloutTable} (
                k DATE NOT NULL,
                v VARCHAR(100) NULL,
                INDEX idx_v (v) USING INVERTED
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
        // The rollout switch is initially false and can only be enabled. Both CREATE TABLE
        // and this ALTER must consume and ignore the partition format property.
        sql """ALTER TABLE ${disabledRolloutTable}
                SET ("partition.inverted_index_storage_format" = "V3")"""
        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "true")
        sql """ALTER TABLE ${disabledRolloutTable}
                ADD PARTITION p_after_disabled VALUES [("2024-01-01"), ("2025-01-01"))"""
        assertPartitionFormat(disabledRolloutTable, "p_after_disabled", "V2")

        // ADD INDEX uses the table-level V2 format; partition overrides do not change
        // the existing VARIANT validation condition.
        sql """
            CREATE TABLE test_cloud_add_index_variant_v2_reapplied (
                k DATE NOT NULL,
                v VARIANT NULL
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
        sql """ALTER TABLE test_cloud_add_index_variant_v2_reapplied
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_cloud_add_index_variant_v2_reapplied
                ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
        sql """ALTER TABLE test_cloud_add_index_variant_v2_reapplied
                SET ("partition.inverted_index_storage_format" = "V2")"""
        sql """ALTER TABLE test_cloud_add_index_variant_v2_reapplied
                ADD PARTITION p_v2_reapplied VALUES [("2025-01-01"), ("2026-01-01"))"""
        ["p_v2": "V2", "p_v3": "V3", "p_v2_reapplied": "V2"].each { name, format ->
            assertPartitionFormat("test_cloud_add_index_variant_v2_reapplied", name, format)
        }
        sql """ALTER TABLE test_cloud_add_index_variant_v2_reapplied
                ADD INDEX idx_variant(v) USING INVERTED"""
        waitForLatestSchemaChangeDone("test_cloud_add_index_variant_v2_reapplied")

        // dict_compression is accepted by FE for a V2 table. The V2 writer ignores
        // the property while still writing and querying the inverted index normally.
        sql """
            CREATE TABLE test_cloud_dict_compression_v2 (
                k INT NOT NULL,
                create_value VARCHAR(100) NULL,
                add_value VARCHAR(100) NULL,
                INDEX idx_create_value (create_value) USING INVERTED PROPERTIES(
                    "parser" = "english",
                    "dict_compression" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            )
        """
        sql """INSERT INTO test_cloud_dict_compression_v2 VALUES
                (1, "create dictionary value", "add dictionary value")"""
        sql "SET enable_add_index_for_new_data = false"
        sql """ALTER TABLE test_cloud_dict_compression_v2
                ADD INDEX idx_add_value(add_value) USING INVERTED PROPERTIES(
                    "parser" = "english",
                    "dict_compression" = "true"
                )"""
        waitForLatestSchemaChangeDone("test_cloud_dict_compression_v2")
        sql "SET enable_inverted_index_query = true"
        assertEquals([[1]], sql("""
            SELECT k FROM test_cloud_dict_compression_v2
            WHERE create_value MATCH_ANY 'create' ORDER BY k
        """))
        assertEquals([[1]], sql("""
            SELECT k FROM test_cloud_dict_compression_v2
            WHERE add_value MATCH_ANY 'add' ORDER BY k
        """))

    }
}
