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

suite("test_partition_cloud_inverted_index_storage_format_rowset_schema", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "true")

        def getBaseTablet = { tableName, partitionName ->
            def tablets = sql_return_maparray("SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
            assertTrue(!tablets.isEmpty())
            def baseTablets = tablets.findAll { tablet ->
                def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
                tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
            assertEquals(1, baseTablets.size())
            return baseTablets[0]
        }

        def assertPartitionFormat = { tableName, partitionName, expectedFormat ->
            def partitions = sql_return_maparray(
                    "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'")
            assertEquals(1, partitions.size())
            assertEquals(expectedFormat, partitions[0].InvertedIndexStorageFormat)

            def tablet = getBaseTablet(tableName, partitionName)
            def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
            assertEquals(expectedFormat, tabletMeta.inverted_index_storage_format)
        }

        def assertNestedIndexFormats = { tableName, partitionName, expectedFormat, expectedDataRowsets ->
            def tablet = getBaseTablet(tableName, partitionName)
            def backendIdToIp = [:]
            def backendIdToHttpPort = [:]
            getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
            def (code, out, err) = http_client("GET", String.format(
                    "http://%s:%s/api/show_nested_index_file?tablet_id=%s",
                    backendIdToIp.get(tablet.BackendId),
                    backendIdToHttpPort.get(tablet.BackendId), tablet.TabletId))
            logger.info("show_nested_index_file tablet=${tablet.TabletId}, code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)

            def nestedIndex = parseJson(out.trim())
            assertEquals(tablet.TabletId.toString(), nestedIndex.tablet_id.toString())
            def dataRowsets = nestedIndex.rowsets.findAll { rowset -> rowset.segments?.size() > 0 }
            assertEquals(expectedDataRowsets, dataRowsets.size())
            dataRowsets.each { rowset ->
                assertEquals(expectedFormat, rowset.index_storage_format)
            }
        }

        def assertRows = { tableName, expectedRows ->
            def rows = sql """
                SELECT CAST(dt AS STRING), id, content
                FROM ${tableName}
                WHERE content MATCH_ANY 'rowset'
                ORDER BY dt, id
            """
            assertEquals(expectedRows, rows)
        }

        sql "DROP TABLE IF EXISTS partition_format_v3_first"
        sql """
            CREATE TABLE partition_format_v3_first (
                dt DATE NOT NULL,
                id INT NOT NULL,
                content STRING,
                INDEX idx_content (content) USING INVERTED PROPERTIES ("parser" = "unicode")
            ) ENGINE = OLAP
            DUPLICATE KEY(dt, id)
            PARTITION BY RANGE(dt) (
                PARTITION p_v3 VALUES LESS THAN ("2024-01-01")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V3"
            )
        """

        // The initial V3 tablet writes the shared (index_id, logical_schema_version) key first.
        sql """INSERT INTO partition_format_v3_first VALUES
                ("2023-06-01", 1, "first v3 rowset")"""
        sql """ALTER TABLE partition_format_v3_first
                SET ("partition.inverted_index_storage_format" = "SNII")"""
        sql """ALTER TABLE partition_format_v3_first
                ADD PARTITION p_snii VALUES [("2024-01-01"), ("2025-01-01"))"""
        sql """INSERT INTO partition_format_v3_first VALUES
                ("2024-06-01", 2, "first snii rowset")"""
        sql """INSERT INTO partition_format_v3_first VALUES
                ("2024-06-02", 3, "second snii rowset")"""

        assertPartitionFormat("partition_format_v3_first", "p_v3", "V3")
        assertPartitionFormat("partition_format_v3_first", "p_snii", "SNII")
        assertRows("partition_format_v3_first", [
            ["2023-06-01", 1, "first v3 rowset"],
            ["2024-06-01", 2, "first snii rowset"],
            ["2024-06-02", 3, "second snii rowset"]
        ])
        assertNestedIndexFormats("partition_format_v3_first", "p_v3", "V3", 1)
        assertNestedIndexFormats("partition_format_v3_first", "p_snii", "SNII", 2)

        sql "DROP TABLE IF EXISTS partition_format_snii_first"
        sql """
            CREATE TABLE partition_format_snii_first (
                dt DATE NOT NULL,
                id INT NOT NULL,
                content STRING,
                INDEX idx_content (content) USING INVERTED PROPERTIES ("parser" = "unicode")
            ) ENGINE = OLAP
            DUPLICATE KEY(dt, id)
            PARTITION BY RANGE(dt) (
                PARTITION p_snii VALUES LESS THAN ("2024-01-01")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "SNII"
            )
        """

        // Reverse the schema KV first-writer order: SNII first, V3 second.
        sql """INSERT INTO partition_format_snii_first VALUES
                ("2023-06-01", 1, "first snii rowset")"""
        sql """ALTER TABLE partition_format_snii_first
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE partition_format_snii_first
                ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
        sql """INSERT INTO partition_format_snii_first VALUES
                ("2024-06-01", 2, "first v3 rowset")"""
        sql """INSERT INTO partition_format_snii_first VALUES
                ("2024-06-02", 3, "second v3 rowset")"""

        assertPartitionFormat("partition_format_snii_first", "p_snii", "SNII")
        assertPartitionFormat("partition_format_snii_first", "p_v3", "V3")
        assertRows("partition_format_snii_first", [
            ["2023-06-01", 1, "first snii rowset"],
            ["2024-06-01", 2, "first v3 rowset"],
            ["2024-06-02", 3, "second v3 rowset"]
        ])
        assertNestedIndexFormats("partition_format_snii_first", "p_snii", "SNII", 1)
        assertNestedIndexFormats("partition_format_snii_first", "p_v3", "V3", 2)
    }
}
