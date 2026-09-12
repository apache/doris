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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.Http

suite("test_partition_cloud_inverted_index_storage_format_stream_load", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "true")

        def getBaseTablets = { tableName, partitionName ->
            def tablets = sql_return_maparray("SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
            assertTrue(!tablets.isEmpty())
            return tablets.findAll { tablet ->
                def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
                tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
        }

        def assertTabletFormat = { tableName, partitionName, expectedFormat ->
            def baseTablets = getBaseTablets(tableName, partitionName)
            assertEquals(1, baseTablets.size())
            baseTablets.each { tablet ->
                def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
                assertEquals(expectedFormat, tabletMeta.inverted_index_storage_format)
            }
        }

        def assertPartitionRowsetsFormat = { tableName, partitionName, expectedFormat, minimumDataRowsets ->
            def baseTablets = getBaseTablets(tableName, partitionName)
            assertEquals(1, baseTablets.size())

            def backendIdToIp = [:]
            def backendIdToHttpPort = [:]
            getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
            baseTablets.each { tablet ->
                def tabletMeta = Http.GET(tablet.MetaUrl, true, false)
                assertEquals(expectedFormat, tabletMeta.inverted_index_storage_format)

                def (code, out, err) = http_client("GET", String.format(
                        "http://%s:%s/api/show_nested_index_file?tablet_id=%s",
                        backendIdToIp.get(tablet.BackendId),
                        backendIdToHttpPort.get(tablet.BackendId), tablet.TabletId))
                logger.info("show_nested_index_file tablet=${tablet.TabletId}, code=${code}, out=${out}, err=${err}")
                assertEquals(0, code)

                def nestedIndex = parseJson(out.trim())
                def rowsets = nestedIndex.rowsets ?: []
                assertTrue(!rowsets.isEmpty())
                rowsets.each { rowset ->
                    assertEquals(expectedFormat, rowset.index_storage_format)
                }
                def dataRowsets = rowsets.findAll { rowset -> rowset.segments?.size() > 0 }
                assertTrue(dataRowsets.size() >= minimumDataRowsets,
                        "expected at least ${minimumDataRowsets} data rowsets, actual: ${dataRowsets.size()}")
            }
        }

        def tableName = "test_partition_inv_idx_format_large_stream_load"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                partition_key VARCHAR(16) NOT NULL,
                id BIGINT NOT NULL,
                content STRING,
                INDEX idx_content (content) USING INVERTED PROPERTIES ("parser" = "unicode")
            ) ENGINE = OLAP
            DUPLICATE KEY(partition_key, id)
            AUTO PARTITION BY LIST(partition_key) ()
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3"
            )
        """

        // Materialize the existing V3 partition with several rowsets before the stream load starts.
        sql "INSERT INTO ${tableName} VALUES ('v3', -1, 'first existing v3 rowset')"
        sql "INSERT INTO ${tableName} VALUES ('v3', -2, 'second existing v3 rowset')"
        sql "INSERT INTO ${tableName} VALUES ('v3', -3, 'third existing v3 rowset')"
        def initialPartitions = sql_return_maparray("SHOW PARTITIONS FROM ${tableName}")
        assertEquals(1, initialPartitions.size())
        def v3PartitionName = initialPartitions[0].PartitionName
        assertTabletFormat(tableName, v3PartitionName, "V3")

        // The iterator blocks between the two partition keys, so a small payload is sufficient
        // to keep one stream-load transaction across the property update.
        final int oldPartitionRows = 10
        final int newPartitionRows = 10
        def oldPartitionRowsSent = new CountDownLatch(1)
        def allowNewPartitionRows = new CountDownLatch(1)
        def rows = new Iterator<List<Object>>() {
            private int row = 0

            @Override
            boolean hasNext() {
                return row < oldPartitionRows + newPartitionRows
            }

            @Override
            List<Object> next() {
                if (row == oldPartitionRows) {
                    oldPartitionRowsSent.countDown()
                    if (!allowNewPartitionRows.await(2, TimeUnit.MINUTES)) {
                        throw new IllegalStateException("timed out waiting to resume V2 partition rows")
                    }
                }
                boolean isExistingV3Partition = row < oldPartitionRows
                def result = [
                    isExistingV3Partition ? "v3" : "v2",
                    row,
                    isExistingV3Partition ? "v3 stream rowset" : "v2 stream rowset"
                ]
                row++
                return result
            }
        }

        // Keep one stream-load transaction open while its input changes from an existing V3
        // partition to the first auto-created V2 partition.
        def streamLoadFuture = extraThread("partition-format-stream-load", {
            streamLoad {
                table tableName
                inputIterator rows
                time 120_000
            }
        })

        assertTrue(oldPartitionRowsSent.await(2, TimeUnit.MINUTES))
        sql """ALTER TABLE ${tableName}
                SET ("partition.inverted_index_storage_format" = "V2")"""
        allowNewPartitionRows.countDown()
        streamLoadFuture.get(5, TimeUnit.MINUTES)

        def partitionsAfterLoad = sql_return_maparray("SHOW PARTITIONS FROM ${tableName}")
        assertEquals(2, partitionsAfterLoad.size())
        def v2Partition = partitionsAfterLoad.find { it.PartitionName != v3PartitionName }
        assertNotNull(v2Partition)
        assertEquals("V2", v2Partition.InvertedIndexStorageFormat)

        // Keep several data rowsets in both partitions. The V3 partition must retain its original
        // format after the table default becomes V2, while every V2 rowset must keep the new format.
        sql "INSERT INTO ${tableName} VALUES ('v2', -4, 'second v2 rowset')"
        sql "INSERT INTO ${tableName} VALUES ('v2', -5, 'third v2 rowset')"
        assertTabletFormat(tableName, v3PartitionName, "V3")

        // The debug endpoint synchronizes visible cloud rowsets itself. Keep this before the
        // queries below so that normal query synchronization cannot mask an endpoint regression.
        assertPartitionRowsetsFormat(tableName, v2Partition.PartitionName, "V2", 3)

        // Search every data rowset in both partitions through the inverted index.
        def v3MatchAnyRows = sql """
            SELECT COUNT(*)
            FROM ${tableName}
            WHERE partition_key = 'v3' AND content MATCH_ANY 'rowset'
        """
        assertEquals([[(oldPartitionRows + 3).toLong()]], v3MatchAnyRows)
        def v2MatchAnyRows = sql """
            SELECT COUNT(*)
            FROM ${tableName}
            WHERE partition_key = 'v2' AND content MATCH_ANY 'rowset'
        """
        assertEquals([[(newPartitionRows + 2).toLong()]], v2MatchAnyRows)

        def rowCounts = sql """
            SELECT partition_key, COUNT(*)
            FROM ${tableName}
            GROUP BY partition_key
            ORDER BY partition_key
        """
        assertEquals([["v2", (newPartitionRows + 2).toLong()], ["v3", (oldPartitionRows + 3).toLong()]], rowCounts)
    }
}
