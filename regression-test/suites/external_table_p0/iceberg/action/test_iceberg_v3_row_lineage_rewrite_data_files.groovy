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

suite("test_iceberg_v3_row_lineage_rewrite_data_files", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_rewrite_data_files"
    String dbName = "test_row_lineage_rewrite_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    String rewriteTable = "test_row_lineage_rewrite_unpartitioned"
    String rewritePartitionTable = "test_row_lineage_rewrite_partitioned"

    def schemaContainsField = { schemaRows, fieldName ->
        String target = fieldName.toLowerCase()
        return schemaRows.any { row -> row.toString().toLowerCase().contains(target) }
    }

    def parquetSchemaRows = { filePath ->
        return sql("""
            select *
            from parquet_meta(
                "uri" = "${filePath}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "endpoint" = "${endpoint}",
                "region" = "us-east-1",
                "mode" = "parquet_schema"
            )
        """)
    }

    def assertCurrentFilesContainRowLineageColumns = { tableName ->
        def files = sql("""select file_path from ${tableName}\$files order by file_path""")
        log.info("Checking rewritten files for physical row lineage columns in ${tableName}: ${files}")
        assertTrue(files.size() > 0, "Current files should exist for ${tableName}")
        files.each { row ->
            def schemaRows = parquetSchemaRows(row[0].toString())
            log.info("Rewritten parquet schema for ${tableName}, file=${row[0]} -> ${schemaRows}")
            assertTrue(schemaContainsField(schemaRows, "_row_id"),
                    "Rewritten file should physically contain _row_id for ${tableName}, schema=${schemaRows}")
            assertTrue(schemaContainsField(schemaRows, "_last_updated_sequence_number"),
                    "Rewritten file should physically contain _last_updated_sequence_number for ${tableName}, schema=${schemaRows}")
        }
    }

    def assertCurrentFilesDoNotContainRowLineageColumns = { tableName ->
        def files = sql("""select file_path from ${tableName}\$files order by file_path""")
        log.info("Checking regular INSERT files for absence of physical row lineage columns in ${tableName}: ${files}")
        assertTrue(files.size() > 0, "Current files should exist for ${tableName}")
        files.each { row ->
            def schemaRows = parquetSchemaRows(row[0].toString())
            log.info("Regular INSERT parquet schema for ${tableName}, file=${row[0]} -> ${schemaRows}")
            assertTrue(!schemaContainsField(schemaRows, "_row_id"),
                    "Normal INSERT file should not contain _row_id for ${tableName}, schema=${schemaRows}")
            assertTrue(!schemaContainsField(schemaRows, "_last_updated_sequence_number"),
                    "Normal INSERT file should not contain _last_updated_sequence_number for ${tableName}, schema=${schemaRows}")
        }
    }

    def lineageMap = { tableName ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        Map<Integer, List<String>> result = [:]
        rows.each { row ->
            result[row[0].toString().toInteger()] = [row[1].toString(), row[2].toString()]
        }
        log.info("Built lineage map for ${tableName}: ${result}")
        return result
    }

    def assertLineageMapEquals = { expected, actual, tableName ->
        log.info("Comparing lineage maps for ${tableName}: expected=${expected}, actual=${actual}")
        assertEquals(expected.size(), actual.size())
        expected.each { key, value ->
            assertTrue(actual.containsKey(key), "Missing id=${key} after rewrite for ${tableName}")
            assertEquals(value[0], actual[key][0])
            assertEquals(value[1], actual[key][1])
        }
    }

    def runRewriteAndAssert = { tableName, expectedCount ->
        def filesBefore = sql("""select file_path from ${tableName}\$files order by file_path""")
        def snapshotsBefore = sql("""select snapshot_id from ${tableName}\$snapshots order by committed_at""")
        log.info("Checking rewrite preconditions for ${tableName}: filesBefore=${filesBefore}, snapshotsBefore=${snapshotsBefore}")
        assertTrue(filesBefore.size() >= 2,
                "Rewrite test requires at least 2 input files for ${tableName}, but got ${filesBefore.size()}")

        def visibleBefore = sql("""select * from ${tableName} order by id""")
        def rowLineageBefore = lineageMap(tableName)
        log.info("Visible rows before rewrite for ${tableName}: ${visibleBefore}")

        assertCurrentFilesDoNotContainRowLineageColumns(tableName)

        def rewriteResult = sql("""
            alter table ${catalogName}.${dbName}.${tableName}
            execute rewrite_data_files(
                "target-file-size-bytes" = "10485760",
                "min-input-files" = "1"
            )
        """)
        log.info("rewrite_data_files result for ${tableName}: ${rewriteResult}")
        assertTrue(rewriteResult.size() > 0, "rewrite_data_files should return summary rows for ${tableName}")
        int rewrittenFiles = rewriteResult[0][0] as int
        assertTrue(rewrittenFiles > 0, "rewrite_data_files should rewrite at least one file for ${tableName}")

        def visibleAfter = sql("""select * from ${tableName} order by id""")
        log.info("Visible rows after rewrite for ${tableName}: ${visibleAfter}")
        assertEquals(visibleBefore, visibleAfter)

        def rowLineageAfter = lineageMap(tableName)
        assertLineageMapEquals(rowLineageBefore, rowLineageAfter, tableName)

        def countAfter = sql("""select count(*) from ${tableName}""")
        log.info("Checking row count after rewrite for ${tableName}: ${countAfter}")
        assertEquals(expectedCount, countAfter[0][0].toString().toInteger())

        def snapshotsAfter = sql("""select snapshot_id from ${tableName}\$snapshots order by committed_at""")
        log.info("Snapshots after rewrite for ${tableName}: ${snapshotsAfter}")
        assertTrue(snapshotsAfter.size() > snapshotsBefore.size(),
                "rewrite_data_files should create a new snapshot for ${tableName}")

        assertCurrentFilesContainRowLineageColumns(tableName)

        def sampleRowId = rowLineageAfter.entrySet().iterator().next().value[0]
        def sampleQuery = sql("""select count(*) from ${tableName} where _row_id = ${sampleRowId}""")
        log.info("Checking sample _row_id predicate after rewrite for ${tableName}: sampleRowId=${sampleRowId}, result=${sampleQuery}")
        assertEquals(1, sampleQuery[0][0].toString().toInteger())
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "us-east-1"
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""
    sql """set show_hidden_columns = false"""

    try {
        sql """drop table if exists ${rewriteTable}"""
        sql """
            create table ${rewriteTable} (
                id int,
                name string,
                score int
            ) engine=iceberg
            properties (
                "format-version" = "3",
                "write.format.default" = "parquet"
            )
        """

        sql """insert into ${rewriteTable} values (1, 'A', 10), (2, 'B', 20)"""
        sql """insert into ${rewriteTable} values (3, 'C', 30), (4, 'D', 40)"""
        sql """insert into ${rewriteTable} values (5, 'E', 50), (6, 'F', 60)"""
        log.info("Inserted three batches into ${rewriteTable} to prepare rewrite_data_files input files")

        // Assert baseline:
        // 1. Data files from regular INSERT do not physically contain the two row lineage columns.
        // 2. After rewrite_data_files, every current data file should contain both row lineage columns.
        // 3. Visible query results stay unchanged before and after rewrite.
        // 4. _row_id and _last_updated_sequence_number stay stable for every row across rewrite.
        runRewriteAndAssert(rewriteTable, 6)

        sql """drop table if exists ${rewritePartitionTable}"""
        sql """
            create table ${rewritePartitionTable} (
                id int,
                name string,
                score int,
                dt date
            ) engine=iceberg
            partition by list (day(dt)) ()
            properties (
                "format-version" = "3",
                "write.format.default" = "parquet"
            )
        """

        sql """insert into ${rewritePartitionTable} values (11, 'P1', 10, '2024-01-01'), (12, 'P2', 20, '2024-01-01')"""
        sql """insert into ${rewritePartitionTable} values (13, 'P3', 30, '2024-01-01'), (14, 'P4', 40, '2024-02-01')"""
        sql """insert into ${rewritePartitionTable} values (15, 'P5', 50, '2024-02-01'), (16, 'P6', 60, '2024-01-01')"""
        log.info("Inserted three partitioned batches into ${rewritePartitionTable} to prepare rewrite_data_files input files")

        // Assert baseline:
        // 1. Partitioned tables also write row lineage columns physically only during rewrite.
        // 2. Business data and row lineage values stay stable before and after rewrite.
        // 3. _row_id predicate queries remain available after rewrite.
        runRewriteAndAssert(rewritePartitionTable, 6)
    } finally {
        sql """drop table if exists ${rewritePartitionTable}"""
        sql """drop table if exists ${rewriteTable}"""
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
