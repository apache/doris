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

suite("test_partition_cloud_inverted_index_storage_format_meta_write_switch", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
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

        def assertTopLevelFormat = { meta, expectedFormat, expectTopLevelFormat, metaName ->
            assertEquals(expectTopLevelFormat, meta.containsKey("inverted_index_storage_format"),
                    "${metaName}: unexpected top-level inverted-index format field")
            if (expectTopLevelFormat) {
                assertEquals(expectedFormat, meta.inverted_index_storage_format,
                        "${metaName}: unexpected top-level inverted-index format")
            }
        }

        def fetchDataRowsetMetasFromBe = { tablet, maxVisibleVersion ->
            def backendIdToIp = [:]
            def backendIdToHttpPort = [:]
            def backendIdToBrpcPort = [:]
            getBackendIpHttpAndBrpcPort(backendIdToIp, backendIdToHttpPort, backendIdToBrpcPort)
            def brpcUrl = "http://${backendIdToIp.get(tablet.BackendId)}:" +
                    "${backendIdToBrpcPort.get(tablet.BackendId)}/doris.PBackendService/get_tablet_rowsets"
            def requestBody = "{\"tablet_id\":${tablet.TabletId},\"version_start\":0," +
                    "\"version_end\":${maxVisibleVersion}}"
            def (code, out, err) = curl("POST", brpcUrl, requestBody)
            assertEquals(0, code,
                    "get_tablet_rowsets failed for tablet ${tablet.TabletId}: out=${out}, err=${err}")
            def response = parseJson(out.trim())
            assertEquals(0, response.status.status_code as int,
                    "get_tablet_rowsets returned an error for tablet ${tablet.TabletId}: ${out}")
            return (response.rowsets ?: []).findAll { (it.num_segments as int) > 0 }
        }

        def assertPartitionMetas = { tableName, partitionName, expectedFormat,
                                    expectTopLevelFormat, expectedDataRowsets ->
            def partitions = sql_return_maparray(
                    "SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = '${partitionName}'")
            assertEquals(1, partitions.size())
            assertEquals(expectedFormat, partitions[0].InvertedIndexStorageFormat)

            def tablet = getBaseTablet(tableName, partitionName)
            def tabletHeader = Http.GET(tablet.MetaUrl, true, false)
            assertTopLevelFormat(tabletHeader, expectedFormat, expectTopLevelFormat,
                    "tablet ${tablet.TabletId}")
            assertEquals(expectedFormat, tabletHeader.schema.inverted_index_storage_format,
                    "tablet ${tablet.TabletId}: unexpected schema inverted-index format")

            def partitionRows = sql """
                SELECT COUNT(*)
                FROM ${tableName} PARTITION (${partitionName})
                WHERE content MATCH_ANY 'rowset'
            """
            assertEquals([[expectedDataRowsets.toLong()]], partitionRows)

            // Read the actual index files and synchronize all visible cloud rowsets on this BE.
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
            def dataRowsets = nestedIndex.rowsets.findAll { rowset -> rowset.segments?.size() > 0 }
            assertEquals(expectedDataRowsets, dataRowsets.size())
            dataRowsets.each { rowset ->
                assertEquals(expectedFormat, rowset.index_storage_format)
            }

            def maxVisibleVersion = partitions[0].VisibleVersion as long
            def rowsetMetas = fetchDataRowsetMetasFromBe(tablet, maxVisibleVersion)
            assertEquals(expectedDataRowsets, rowsetMetas.size())
            rowsetMetas.each { rowsetMeta ->
                assertTopLevelFormat(rowsetMeta, expectedFormat, expectTopLevelFormat,
                        "tablet ${tablet.TabletId} rowset ${rowsetMeta.rowset_id_v2}")
            }

        }

        def assertMatchAnyRows = { tableName, expectedRows ->
            def matchedRows = sql """
                SELECT COUNT(*)
                FROM ${tableName}
                WHERE content MATCH_ANY 'rowset'
            """
            assertEquals([[expectedRows.toLong()]], matchedRows)
        }

        def autoPartitionName = { date ->
            def partitionName = sql """
                SELECT auto_partition_name('range', 'year', '${date}')
            """
            assertEquals(1, partitionName.size())
            return partitionName[0][0]
        }

        def createAutoRangeTable = { tableName ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    dt DATE NOT NULL,
                    id INT NOT NULL,
                    content STRING,
                    INDEX idx_content (content) USING INVERTED PROPERTIES ("parser" = "unicode")
                ) ENGINE = OLAP
                DUPLICATE KEY(dt, id)
                AUTO PARTITION BY RANGE (date_trunc(dt, 'year')) ()
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "inverted_index_storage_format" = "V3"
                )
            """
        }

        // Rollout disabled: partition property changes are ignored by the write path. No new
        // top-level format field is written to either TabletMeta or RowsetMeta in Meta Service.
        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "false")
        def disabledTable = "test_partition_inv_idx_format_meta_switch_disabled"
        createAutoRangeTable(disabledTable)

        sql """INSERT INTO ${disabledTable} VALUES
                ("2023-06-01", 1, "first disabled p1 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2024-06-01", 2, "first disabled p2 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2025-06-01", 3, "first disabled p3 rowset")"""
        sql """ALTER TABLE ${disabledTable}
                SET ("partition.inverted_index_storage_format" = "V2")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2023-06-02", 4, "second disabled p1 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2024-06-02", 5, "second disabled p2 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2025-06-02", 6, "second disabled p3 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2026-06-01", 7, "first disabled p4 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2026-06-02", 8, "second disabled p4 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2027-06-01", 9, "first disabled p5 rowset")"""
        sql """INSERT INTO ${disabledTable} VALUES
                ("2027-06-02", 10, "second disabled p5 rowset")"""

        // The 2026 and 2027 partitions are created by their first INSERT after the property
        // change. With rollout disabled they still use the table-level V3 format.
        ["2023-06-01", "2024-06-01", "2025-06-01", "2026-06-01", "2027-06-01"].each { date ->
            def partitionName = autoPartitionName(date)
            assertPartitionMetas(disabledTable, partitionName, "V3", false, 2)
        }
        assertMatchAnyRows(disabledTable, 10)

        // The switch is one way. Existing tablets keep their schema-only V3 representation;
        // only tablets created after enabling the switch persist their top-level format.
        def enabledTable = "test_partition_inv_idx_format_meta_switch_enabled"
        createAutoRangeTable(enabledTable)
        sql """INSERT INTO ${enabledTable} VALUES
                ("2023-06-01", 1, "first old partition rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2023-06-02", 2, "second old partition rowset")"""

        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "true")
        sql """INSERT INTO ${enabledTable} VALUES
                ("2023-06-03", 3, "third old partition rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2024-06-01", 4, "first v3 p1 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2024-06-02", 5, "second v3 p1 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2025-06-01", 6, "first v3 p2 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2025-06-02", 7, "second v3 p2 rowset")"""

        sql """ALTER TABLE ${enabledTable}
                SET ("partition.inverted_index_storage_format" = "V2")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2026-06-01", 8, "first v2 p1 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2026-06-02", 9, "second v2 p1 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2027-06-01", 10, "first v2 p2 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2027-06-02", 11, "second v2 p2 rowset")"""

        sql """ALTER TABLE ${enabledTable}
                SET ("partition.inverted_index_storage_format" = "SNII")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2028-06-01", 12, "first snii p1 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2028-06-02", 13, "second snii p1 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2029-06-01", 14, "first snii p2 rowset")"""
        sql """INSERT INTO ${enabledTable} VALUES
                ("2029-06-02", 15, "second snii p2 rowset")"""

        assertPartitionMetas(enabledTable, autoPartitionName("2023-06-01"), "V3", false, 3)
        ["2024-06-01", "2025-06-01"].each { date ->
            def partitionName = autoPartitionName(date)
            assertPartitionMetas(enabledTable, partitionName, "V3", true, 2)
        }
        ["2026-06-01", "2027-06-01"].each { date ->
            def partitionName = autoPartitionName(date)
            assertPartitionMetas(enabledTable, partitionName, "V2", true, 2)
        }
        ["2028-06-01", "2029-06-01"].each { date ->
            def partitionName = autoPartitionName(date)
            assertPartitionMetas(enabledTable, partitionName, "SNII", true, 2)
        }
        assertMatchAnyRows(enabledTable, 15)
    }
}
