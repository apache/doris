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

suite("test_iceberg_v3_row_lineage_dml_mode_matrix", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_dml_mode_matrix"
    String dbName = "test_row_lineage_dml_mode_matrix_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def formatVersions = [2, 3]
    def partitionFlags = [false, true]
    def scenarios = [
            [name: "m01_delete_cow", operation: "delete", property: "write.delete.mode", mode: "copy-on-write"],
            [name: "m02_delete_mor", operation: "delete", property: "write.delete.mode", mode: "merge-on-read"],
            [name: "m03_update_cow", operation: "update", property: "write.update.mode", mode: "copy-on-write"],
            [name: "m04_update_mor", operation: "update", property: "write.update.mode", mode: "merge-on-read"],
            [name: "m05_merge_cow", operation: "merge", property: "write.merge.mode", mode: "copy-on-write"],
            [name: "m06_merge_mor", operation: "merge", property: "write.merge.mode", mode: "merge-on-read"]
    ]
    def initialRows = [
            [1, "Alice", 10, "2024-01-01"],
            [2, "Bob", 20, "2024-01-02"],
            [3, "Carol", 30, "2024-01-03"]
    ]

    def createTable = { tableName, formatVersion, format, partitioned, propertyName, mode ->
        sql """drop table if exists ${tableName}"""
        String partitionClause = partitioned ? "partition by list (day(dt)) ()" : ""
        sql """
            create table ${tableName} (
                id int,
                name string,
                score int,
                dt date
            ) engine=iceberg
            ${partitionClause}
            properties (
                "format-version" = "${formatVersion}",
                "write.format.default" = "${format}",
                "${propertyName}" = "${mode}"
            )
        """
        sql """
            insert into ${tableName} values
            (1, 'Alice', 10, date '2024-01-01'),
            (2, 'Bob', 20, date '2024-01-02'),
            (3, 'Carol', 30, date '2024-01-03')
        """
    }

    def lineageMap = { tableName ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        Map<Integer, List<Long>> result = [:]
        rows.each { row ->
            result[row[0].toString().toInteger()] = [row[1].toString().toLong(), row[2].toString().toLong()]
        }
        log.info("Lineage map for ${tableName}: ${result}")
        return result
    }

    def assertV3LineageNonNull = { tableName, expectedIds ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        log.info("Checking v3 row lineage rows for ${tableName}: ${rows}")
        assertEquals(expectedIds.size(), rows.size())
        for (int i = 0; i < expectedIds.size(); i++) {
            assertEquals(expectedIds[i], rows[i][0].toString().toInteger())
            assertTrue(rows[i][1] != null, "_row_id should be non-null for ${tableName}, row=${rows[i]}")
            assertTrue(rows[i][2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName}, row=${rows[i]}")
        }
    }

    def assertDeleteFilesForVersion = { tableName, formatVersion, format, scenario ->
        def deleteFiles = sql("""
            select file_path, lower(file_format), record_count
            from ${tableName}\$delete_files
            order by file_path
        """)
        log.info("Checking delete files for ${tableName}, scenario=${scenario.name}: ${deleteFiles}")
        if (scenario.mode == "copy-on-write") {
            assertEquals(0, deleteFiles.size(),
                "COW scenario ${scenario.name} should not create delete files for ${tableName}, actual=${deleteFiles}")
        }
        if (scenario.mode == "merge-on-read" && formatVersion == 3) {
            assertTrue(deleteFiles.size() > 0,
                    "MOR scenario ${scenario.name} should create delete files for ${tableName}")
        }
        deleteFiles.each { row ->
            String filePath = row[0].toString().toLowerCase()
            String fileFormat = row[1].toString().toLowerCase()
            if (formatVersion == 3) {
                assertEquals("puffin", fileFormat)
                assertTrue(filePath.endsWith(".puffin"),
                        "v3 delete file should be Puffin for ${tableName}, row=${row}")
            } else {
                assertEquals(format, fileFormat)
                assertTrue(filePath.contains("delete_pos"),
                        "v2 delete file should be position delete for ${tableName}, row=${row}")
                assertTrue(filePath.endsWith(format == "parquet" ? ".parquet" : ".orc"),
                        "v2 delete file suffix should match ${format} for ${tableName}, row=${row}")
            }
        }
    }

    def assertBusinessRows = { tableName, expected ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkRows = spark_iceberg("""
            select id, name, score, dt
            from demo.${dbName}.${tableName}
            order by id
        """)
        def dorisRows = sql("""select id, name, score, dt from ${tableName} order by id""")
        log.info("Spark business rows for ${tableName}: ${sparkRows}")
        log.info("Doris business rows for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)

        def actual = dorisRows.collect { row ->
            [row[0].toString().toInteger(), row[1].toString(), row[2].toString().toInteger(), row[3].toString()]
        }
        assertEquals(expected, actual)
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
        scenarios.each { scenario ->
            formatVersions.each { formatVersion ->
                formats.each { format ->
                    partitionFlags.each { partitioned ->
                        String partitionSuffix = partitioned ? "part" : "unpart"
                        String tableName = "${scenario.name}_v${formatVersion}_${format}_${partitionSuffix}"
                        log.info("Running DML mode scenario ${scenario} on ${tableName}")
                        try {
                            createTable(tableName, formatVersion, format, partitioned, scenario.property, scenario.mode)
                            Map<Integer, List<Long>> beforeLineage = formatVersion == 3 ? lineageMap(tableName) : [:]

                            String dmlSql
                            def expectedRows
                            def expectedIds
                            if (scenario.operation == "delete") {
                                dmlSql = """delete from ${tableName} where id = 2"""
                                expectedRows = [
                                        [1, "Alice", 10, "2024-01-01"],
                                        [3, "Carol", 30, "2024-01-03"]
                                ]
                                expectedIds = [1, 3]
                            } else if (scenario.operation == "update") {
                                dmlSql = """update ${tableName} set name = 'Alice_u', score = score + 100 where id = 1"""
                                expectedRows = [
                                        [1, "Alice_u", 110, "2024-01-01"],
                                        [2, "Bob", 20, "2024-01-02"],
                                        [3, "Carol", 30, "2024-01-03"]
                                ]
                                expectedIds = [1, 2, 3]
                            } else {
                                dmlSql = """
                                    merge into ${tableName} t
                                    using (
                                        select 1 as id, 'Alice_m' as name, 111 as score, date '2024-01-01' as dt, 'U' as flag
                                        union all
                                        select 3, 'Carol', 30, date '2024-01-03', 'D'
                                        union all
                                        select 5, 'Eve', 50, date '2024-01-05', 'I'
                                    ) s
                                    on t.id = s.id
                                    when matched and s.flag = 'D' then delete
                                    when matched then update set name = s.name, score = s.score
                                    when not matched then insert (id, name, score, dt)
                                    values (s.id, s.name, s.score, s.dt)
                                """
                                expectedRows = [
                                        [1, "Alice_m", 111, "2024-01-01"],
                                        [2, "Bob", 20, "2024-01-02"],
                                        [5, "Eve", 50, "2024-01-05"]
                                ]
                                expectedIds = [1, 2, 5]
                            }

                            if (scenario.mode == "copy-on-write") {
                                String operationName = scenario.operation == "merge"
                                        ? "MERGE INTO" : scenario.operation.toUpperCase()
                                test {
                                    sql dmlSql
                                    exception "Doris does not support ${operationName} on Iceberg copy-on-write tables"
                                    exception "Set table property '${scenario.property}' to 'merge-on-read'"
                                }
                                assertBusinessRows(tableName, initialRows)
                                if (formatVersion == 3) {
                                    assertV3LineageNonNull(tableName, [1, 2, 3])
                                    assertEquals(beforeLineage, lineageMap(tableName))
                                }
                            } else {
                                sql dmlSql
                                assertBusinessRows(tableName, expectedRows)
                                if (formatVersion == 3) {
                                    assertV3LineageNonNull(tableName, expectedIds)
                                    Map<Integer, List<Long>> afterLineage = lineageMap(tableName)
                                    if (scenario.operation == "delete") {
                                        assertEquals(beforeLineage[1], afterLineage[1])
                                        assertEquals(beforeLineage[3], afterLineage[3])
                                        assertTrue(!afterLineage.containsKey(2))
                                    } else if (scenario.operation == "update") {
                                        assertEquals(beforeLineage[1][0], afterLineage[1][0])
                                        assertTrue(afterLineage[1][1] > beforeLineage[1][1],
                                                "UPDATE should advance sequence for id=1 in ${tableName}")
                                        assertEquals(beforeLineage[3], afterLineage[3])
                                    } else {
                                        assertEquals(beforeLineage[1][0], afterLineage[1][0])
                                        assertTrue(afterLineage[1][1] > beforeLineage[1][1],
                                                "MERGE UPDATE should advance sequence for id=1 in ${tableName}")
                                        assertEquals(beforeLineage[2], afterLineage[2])
                                        assertTrue(!afterLineage.containsKey(3))
                                        assertTrue(afterLineage[5][0] != null)
                                    }
                                }
                            }

                            assertDeleteFilesForVersion(tableName, formatVersion, format, scenario)
                        } finally {
                            sql """drop table if exists ${tableName}"""
                        }
                    }
                }
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
