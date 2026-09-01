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

suite("test_partition_cloud_inverted_index_storage_format_rollout", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs += [
        'enable_debug_points=true',
    ]

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
            def baseTabletMetas = tablets.collect { tablet ->
                Http.GET(tablet.MetaUrl, true, false)
            }.findAll { tabletMeta ->
                tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
            assertEquals(1, baseTabletMetas.size())
            baseTabletMetas.each { tabletMeta ->
                assertEquals(expectedFormat, tabletMeta.inverted_index_storage_format)
            }
        }

        def getBaseTabletMeta = { tableName, partitionName ->
            def tablets = sql_return_maparray("SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
            assertTrue(!tablets.isEmpty())
            def baseTabletMetas = tablets.collect { tablet ->
                Http.GET(tablet.MetaUrl, true, false)
            }.findAll { tabletMeta ->
                tabletMeta.schema.index?.any { index -> index.index_type == "INVERTED" }
            }
            assertEquals(1, baseTabletMetas.size())
            return baseTabletMetas[0]
        }

        def assertBaseTabletColumns = { tableName, partitionName, expectedColumns ->
            def actualColumns = getBaseTabletMeta(tableName, partitionName).schema.column.collect { column ->
                [column.name, column.type == "DATEV2" ? "DATE" : column.type]
            }
            assertEquals(expectedColumns, actualColumns)
        }

        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_rollout"
        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_snii_rollout"
        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_initial"
        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_list"
        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_auto"
        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_schema_change"
        sql "DROP TABLE IF EXISTS test_cloud_partition_inverted_index_storage_format_dynamic"

        sql """
            CREATE TABLE test_cloud_partition_inverted_index_storage_format_rollout (
                k DATE NOT NULL,
                v VARCHAR(100) NULL,
                INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            PARTITION BY RANGE(k) (
                PARTITION p_old VALUES LESS THAN ("2024-01-01")
            )
        DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            )
        """

        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_rollout ADD PARTITION p_default VALUES [("2026-01-01"), ("2027-01-01"))"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_old", "V2")
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_default", "V2")

        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_rollout SET ("partition.inverted_index_storage_format" = "V3")"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_old", "V2")
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_rollout ADD PARTITION p_new VALUES [("2024-01-01"), ("2025-01-01"))"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_new", "V3")

        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_rollout SET ("partition.inverted_index_storage_format" = "V2")"""
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_rollout ADD PARTITION p_downgrade VALUES [("2025-01-01"), ("2026-01-01"))"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_downgrade", "V2")

        sql "set enable_memtable_on_sink_node = true"
        try {
            sql """
                INSERT INTO test_cloud_partition_inverted_index_storage_format_rollout VALUES
                    ("2023-01-01", "old token"),
                    ("2024-01-01", "new token"),
                    ("2025-01-01", "downgrade token")
            """
        } finally {
            sql "set enable_memtable_on_sink_node = false"
        }
        def mixedFormatRows = sql """
            SELECT CAST(k AS STRING), v
            FROM test_cloud_partition_inverted_index_storage_format_rollout
            WHERE v MATCH_ANY 'token' ORDER BY k
        """
        assertEquals([
            ["2023-01-01", "old token"],
            ["2024-01-01", "new token"],
            ["2025-01-01", "downgrade token"]
        ], mixedFormatRows)

        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_rollout SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """TRUNCATE TABLE test_cloud_partition_inverted_index_storage_format_rollout PARTITION p_old"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_old", "V3")
        sql """INSERT OVERWRITE TABLE test_cloud_partition_inverted_index_storage_format_rollout PARTITION(p_old) VALUES ("2023-01-02", "old rewritten")"""
        def rewrittenRows = sql """
            SELECT CAST(k AS STRING), v
            FROM test_cloud_partition_inverted_index_storage_format_rollout
            WHERE v MATCH_ANY 'rewritten' ORDER BY k
        """
        assertEquals([["2023-01-02", "old rewritten"]], rewrittenRows)
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_rollout", "p_old", "V3")

        // Roll out SNII after V2 and V3 partitions already exist. Each partition must
        // retain the storage format assigned when it was created.
        sql """
            CREATE TABLE test_cloud_partition_inverted_index_storage_format_snii_rollout (
                k DATE NOT NULL,
                v VARCHAR(100) NULL,
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
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_snii_rollout
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_snii_rollout
                ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_snii_rollout
                SET ("partition.inverted_index_storage_format" = "SNII")"""
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_snii_rollout
                ADD PARTITION p_snii VALUES [("2025-01-01"), ("2026-01-01"))"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_snii_rollout", "p_v2", "V2")
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_snii_rollout", "p_v3", "V3")
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_snii_rollout", "p_snii", "SNII")

        sql """
            INSERT INTO test_cloud_partition_inverted_index_storage_format_snii_rollout VALUES
                ("2023-01-01", "v2 rollout token"),
                ("2024-01-01", "v3 rollout token"),
                ("2025-01-01", "snii rollout token")
        """
        def sniiRolloutRows = sql """
            SELECT CAST(k AS STRING), v
            FROM test_cloud_partition_inverted_index_storage_format_snii_rollout
            WHERE v MATCH_ANY 'rollout' ORDER BY k
        """
        assertEquals([
            ["2023-01-01", "v2 rollout token"],
            ["2024-01-01", "v3 rollout token"],
            ["2025-01-01", "snii rollout token"]
        ], sniiRolloutRows)

        sql """
            CREATE TABLE test_cloud_partition_inverted_index_storage_format_initial (
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
                "test_cloud_partition_inverted_index_storage_format_initial", "p_initial", "V3")

        sql """
            CREATE TABLE test_cloud_partition_inverted_index_storage_format_list (
                k INT NOT NULL,
                category VARCHAR(20) NOT NULL,
                v VARCHAR(100) NULL,
                INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(k, category)
            PARTITION BY LIST(category) (
                PARTITION p_old VALUES IN ("old")
            )
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            )
        """
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_list", "p_old", "V2")

        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_list
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_list
                ADD PARTITION p_new VALUES IN ("new")"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_list", "p_old", "V2")
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_list", "p_new", "V3")

        sql """
            INSERT INTO test_cloud_partition_inverted_index_storage_format_list VALUES
                (1, "old", "old list token"),
                (2, "new", "new list token")
        """
        def listRows = sql """
            SELECT category, v
            FROM test_cloud_partition_inverted_index_storage_format_list
            WHERE v MATCH_ANY 'token' ORDER BY category
        """
        assertEquals([[
            "new", "new list token"
        ], [
            "old", "old list token"
        ]], listRows)

        sql """
            CREATE TABLE test_cloud_partition_inverted_index_storage_format_auto (
                k VARCHAR(20) NOT NULL,
                v VARCHAR(100) NULL,
                INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            AUTO PARTITION BY LIST(k) ()
        DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            )
        """
        sql """INSERT INTO test_cloud_partition_inverted_index_storage_format_auto VALUES ("old", "auto old")"""
        def autoPartitions = sql_return_maparray(
                "SHOW PARTITIONS FROM test_cloud_partition_inverted_index_storage_format_auto")
        assertEquals(1, autoPartitions.size())
        def oldAutoPartitionName = autoPartitions[0].PartitionName
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_auto", oldAutoPartitionName, "V2")

        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_auto SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """INSERT INTO test_cloud_partition_inverted_index_storage_format_auto VALUES ("new", "auto new")"""
        autoPartitions = sql_return_maparray(
                "SHOW PARTITIONS FROM test_cloud_partition_inverted_index_storage_format_auto")
        assertEquals(2, autoPartitions.size())
        def newAutoPartition = autoPartitions.find { it.PartitionName != oldAutoPartitionName }
        assertNotNull(newAutoPartition)
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_auto", newAutoPartition.PartitionName, "V3")
        def autoRows = sql """
            SELECT k, v FROM test_cloud_partition_inverted_index_storage_format_auto
            WHERE v MATCH_ANY 'auto' ORDER BY k
        """
        assertEquals([["new", "auto new"], ["old", "auto old"]], autoRows)

        sql """
            CREATE TABLE test_cloud_partition_inverted_index_storage_format_schema_change (
                k DATE NOT NULL,
                c1 INT NULL,
                c2 VARCHAR(100) NULL,
                INDEX idx_c2 (c2) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            PARTITION BY RANGE(k) (
                PARTITION p_v2 VALUES LESS THAN ("2024-01-01")
            )
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2",
                "light_schema_change" = "true"
            )
        """
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_schema_change
                ADD COLUMN c3 INT NULL"""
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2",
                [["k", "DATE"], ["c1", "INT"], ["c2", "VARCHAR"]])
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_schema_change
                SET ("partition.inverted_index_storage_format" = "V3")"""
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_schema_change
                ADD PARTITION p_v3 VALUES [("2024-01-01"), ("2025-01-01"))"""
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v3",
                [["k", "DATE"], ["c1", "INT"], ["c2", "VARCHAR"], ["c3", "INT"]])
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_schema_change
                DROP PARTITION p_v2"""
        sql """RECOVER PARTITION p_v2 FROM test_cloud_partition_inverted_index_storage_format_schema_change"""
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2", "V2")
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2",
                [["k", "DATE"], ["c1", "INT"], ["c2", "VARCHAR"]])
        sql """
            INSERT INTO test_cloud_partition_inverted_index_storage_format_schema_change (k, c1, c2) VALUES
                ("2023-01-01", 1, "v2 schema change token")
        """
        sql """
            INSERT INTO test_cloud_partition_inverted_index_storage_format_schema_change VALUES
                ("2024-01-01", 2, "v3 schema change token", 3)
        """
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_schema_change
                ADD ROLLUP r_schema_change(c1, c2)"""
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE ROLLUP
                    WHERE TableName = 'test_cloud_partition_inverted_index_storage_format_schema_change'
                    ORDER BY CreateTime DESC LIMIT 1"""
            time 600
        }
        def pV2SourceSchemaVersion = getBaseTabletMeta(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2").schema.schema_version
        def pV3SourceSchemaVersion = getBaseTabletMeta(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v3").schema.schema_version
        assertTrue(pV2SourceSchemaVersion < pV3SourceSchemaVersion)
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2",
                [["k", "DATE"], ["c1", "INT"], ["c2", "VARCHAR"]])
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v3",
                [["k", "DATE"], ["c1", "INT"], ["c2", "VARCHAR"], ["c3", "INT"]])

        def assertShadowIndexes = { partitionName ->
            def partitions = sql_return_maparray(
                    "SHOW PARTITIONS FROM test_cloud_partition_inverted_index_storage_format_schema_change "
                            + "WHERE PartitionName = '${partitionName}'")
            assertEquals(1, partitions.size())
            def tablets = sql_return_maparray(
                    "SHOW TABLETS FROM test_cloud_partition_inverted_index_storage_format_schema_change "
                            + "PARTITION(${partitionName})")
            assertTrue(!tablets.isEmpty())
            def tabletInfo = sql_return_maparray("SHOW TABLET ${tablets[0].TabletId}")
            assertEquals(1, tabletInfo.size())
            def procPath = "/dbs/${tabletInfo[0].DbId}/${tabletInfo[0].TableId}/partitions/${partitions[0].PartitionId}"
            def indexes = sql_return_maparray("SHOW PROC \"${procPath}\"")
            assertEquals(4, indexes.size())
            assertTrue(indexes.any { it.IndexName == "test_cloud_partition_inverted_index_storage_format_schema_change" })
            assertTrue(indexes.any { it.IndexName == "r_schema_change" })
            def shadowIndexes = indexes.findAll { it.State == "SHADOW" }
            assertEquals(2, shadowIndexes.size())
            assertTrue(shadowIndexes.any {
                it.IndexName == "__doris_shadow_test_cloud_partition_inverted_index_storage_format_schema_change"
            })
            assertTrue(shadowIndexes.any { it.IndexName == "__doris_shadow_r_schema_change" })
        }

        GetDebugPoint().enableDebugPointForAllFEs("FE.SchemaChangeJobV2.runRunning.block")
        sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_schema_change
                MODIFY COLUMN c1 BIGINT NULL"""
        def schemaChangeState = ""
        for (def retry = 0; retry < 120; retry++) {
            def jobs = sql_return_maparray("""SHOW ALTER TABLE COLUMN
                    WHERE TableName = 'test_cloud_partition_inverted_index_storage_format_schema_change'
                    ORDER BY CreateTime DESC LIMIT 1""")
            if (!jobs.isEmpty()) {
                schemaChangeState = jobs[0].State
                if (schemaChangeState == "RUNNING") {
                    break
                }
            }
            sleep(1000)
        }
        assertEquals("RUNNING", schemaChangeState)
        assertShadowIndexes("p_v2")
        assertShadowIndexes("p_v3")

        GetDebugPoint().disableDebugPointForAllFEs("FE.SchemaChangeJobV2.runRunning.block")
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN
                    WHERE TableName = 'test_cloud_partition_inverted_index_storage_format_schema_change'
                    ORDER BY CreateTime DESC LIMIT 1"""
            time 600
        }
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2", "V2")
        assertPartitionFormat(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v3", "V3")
        def pV2ShadowSchemaVersion = getBaseTabletMeta(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2").schema.schema_version
        def pV3ShadowSchemaVersion = getBaseTabletMeta(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v3").schema.schema_version
        assertTrue(pV2ShadowSchemaVersion > pV2SourceSchemaVersion)
        assertTrue(pV3ShadowSchemaVersion > pV3SourceSchemaVersion)
        // The schema version identifies the shared logical schema. V2 and V3 are
        // carried by tablet meta, so one schema change gives both shadow tablets
        // the same new logical schema version.
        assertEquals(pV2ShadowSchemaVersion, pV3ShadowSchemaVersion)
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v2",
                [["k", "DATE"], ["c1", "BIGINT"], ["c2", "VARCHAR"], ["c3", "INT"]])
        assertBaseTabletColumns(
                "test_cloud_partition_inverted_index_storage_format_schema_change", "p_v3",
                [["k", "DATE"], ["c1", "BIGINT"], ["c2", "VARCHAR"], ["c3", "INT"]])
        def schemaChangeRows = sql """
            SELECT CAST(k AS STRING), c2
            FROM test_cloud_partition_inverted_index_storage_format_schema_change
            WHERE c2 MATCH_ANY 'schema change token' ORDER BY k
        """
        assertEquals([
            ["2023-01-01", "v2 schema change token"],
            ["2024-01-01", "v3 schema change token"]
        ], schemaChangeRows)

        def oldDynamicPartitionCheckInterval = getFeConfig('dynamic_partition_check_interval_seconds')
        try {
            setFeConfig('dynamic_partition_check_interval_seconds', 1)
            sql """
                CREATE TABLE test_cloud_partition_inverted_index_storage_format_dynamic (
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
                    "SHOW PARTITIONS FROM test_cloud_partition_inverted_index_storage_format_dynamic")
            assertEquals(3, dynamicPartitions.size())
            def initialDynamicPartitionNames = dynamicPartitions.collect { it.PartitionName }
            initialDynamicPartitionNames.each { partitionName ->
                assertPartitionFormat(
                        "test_cloud_partition_inverted_index_storage_format_dynamic", partitionName, "V2")
            }

            sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_dynamic SET ("partition.inverted_index_storage_format" = "V3")"""
            sql """ALTER TABLE test_cloud_partition_inverted_index_storage_format_dynamic SET ("dynamic_partition.end" = "3")"""
            for (def retry = 0; retry < 120; retry++) {
                dynamicPartitions = sql_return_maparray(
                        "SHOW PARTITIONS FROM test_cloud_partition_inverted_index_storage_format_dynamic")
                if (dynamicPartitions.size() == 5) {
                    break
                }
                sleep(1000)
            }
            assertEquals(5, dynamicPartitions.size())
            dynamicPartitions.each { partition ->
                def expectedFormat = initialDynamicPartitionNames.contains(partition.PartitionName) ? "V2" : "V3"
                assertPartitionFormat(
                        "test_cloud_partition_inverted_index_storage_format_dynamic",
                        partition.PartitionName, expectedFormat)
            }
        } finally {
            setFeConfig('dynamic_partition_check_interval_seconds', oldDynamicPartitionCheckInterval)
        }
    }
}
