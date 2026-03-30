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

suite("test_iceberg_v3_row_lineage_query_insert", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_query_insert"
    String dbName = "test_row_lineage_query_insert_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]

    def collectDescColumns = { rows ->
        return rows.collect { row -> row[0].toString().toLowerCase() }
    }

    def schemaContainsField = { schemaRows, fieldName ->
        String target = fieldName.toLowerCase()
        return schemaRows.any { row -> row.toString().toLowerCase().contains(target) }
    }

    def fileSchemaRows = { filePath, format ->
        return sql("""
            desc function s3(
                "uri" = "${filePath}",
                "format" = "${format}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "${endpoint}",
                "s3.region" = "us-east-1"
            )
        """)
    }

    def assertCurrentFilesDoNotContainRowLineageColumns = { tableName, format, messagePrefix ->
        def files = sql("""select file_path, lower(file_format) from ${tableName}\$files order by file_path""")
        log.info("${messagePrefix}: checking ${files.size()} current data files for ${tableName}: ${files}")
        assertTrue(files.size() > 0, "Current data files should exist for ${tableName}")
        files.each { row ->
            assertEquals(format, row[1].toString())
            assertTrue(row[0].toString().endsWith(format == "parquet" ? ".parquet" : ".orc"),
                    "${messagePrefix} should write ${format} files for ${tableName}, file=${row[0]}")
            def schemaRows = fileSchemaRows(row[0].toString(), format)
            log.info("${messagePrefix}: ${format} schema for ${tableName}, file=${row[0]} -> ${schemaRows}")
            assertTrue(!schemaContainsField(schemaRows, "_row_id"),
                    "${messagePrefix} should not physically write _row_id, schema=${schemaRows}")
            assertTrue(!schemaContainsField(schemaRows, "_last_updated_sequence_number"),
                    "${messagePrefix} should not physically write _last_updated_sequence_number, schema=${schemaRows}")
        }
    }

    def assertRowLineageHiddenColumns = { tableName, visibleColumnCount ->
        sql("""set show_hidden_columns = false""")
        def descDefault = sql("""desc ${tableName}""")
        def defaultColumns = collectDescColumns(descDefault)
        log.info("Checking hidden-column default visibility for ${tableName}: desc=${descDefault}")
        assertTrue(!defaultColumns.contains("_row_id"),
                "DESC default should hide _row_id for ${tableName}, got ${defaultColumns}")
        assertTrue(!defaultColumns.contains("_last_updated_sequence_number"),
                "DESC default should hide _last_updated_sequence_number for ${tableName}, got ${defaultColumns}")

        def selectVisible = sql("""select * from ${tableName} order by id""")
        log.info("Checking visible SELECT * layout for ${tableName}: rowCount=${selectVisible.size()}, firstRow=${selectVisible ? selectVisible[0] : 'EMPTY'}")
        assertTrue(selectVisible.size() > 0, "SELECT * should return rows for ${tableName}")
        assertEquals(visibleColumnCount, selectVisible[0].size())

        sql("""set show_hidden_columns = true""")
        def descHidden = sql("""desc ${tableName}""")
        def hiddenColumns = collectDescColumns(descHidden)
        log.info("Checking hidden-column enabled visibility for ${tableName}: desc=${descHidden}")
        assertTrue(hiddenColumns.contains("_row_id"),
                "DESC with show_hidden_columns=true should expose _row_id for ${tableName}, got ${hiddenColumns}")
        assertTrue(hiddenColumns.contains("_last_updated_sequence_number"),
                "DESC with show_hidden_columns=true should expose _last_updated_sequence_number for ${tableName}, got ${hiddenColumns}")

        def selectHidden = sql("""select * from ${tableName} order by id""")
        log.info("Checking hidden SELECT * layout for ${tableName}: rowCount=${selectHidden.size()}, firstRow=${selectHidden ? selectHidden[0] : 'EMPTY'}")
        assertTrue(selectHidden.size() > 0, "SELECT * with hidden columns should return rows for ${tableName}")
        assertEquals(visibleColumnCount + 2 + 1, selectHidden[0].size()) // _row_id + _last_updated_sequence_number + __DORIS_ICEBERG_ROWID_COL__

        sql("""set show_hidden_columns = false""")
    }

    def assertExplicitRowLineageReadable = { tableName, expectedIds ->
        def rowLineageRows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        log.info("Checking explicit row lineage projection for ${tableName}: rows=${rowLineageRows}")
        assertEquals(expectedIds.size(), rowLineageRows.size())
        for (int i = 0; i < expectedIds.size(); i++) {
            assertEquals(expectedIds[i], rowLineageRows[i][0].toString().toInteger())
            assertTrue(rowLineageRows[i][1] != null,
                    "_row_id should be non-null for ${tableName}, row=${rowLineageRows[i]}")
            assertTrue(rowLineageRows[i][2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName}, row=${rowLineageRows[i]}")
        }

        long firstRowId = rowLineageRows[0][1].toString().toLong()
        long secondRowId = rowLineageRows[1][1].toString().toLong()
        assertTrue(firstRowId < secondRowId,
                "Row lineage ids should increase with row position for ${tableName}, rows=${rowLineageRows}")

        def byRowId = sql("""select id from ${tableName} where _row_id = ${firstRowId} order by id""")
        log.info("Checking single _row_id predicate for ${tableName}: rowId=${firstRowId}, result=${byRowId}")
        assertEquals(1, byRowId.size())
        assertEquals(expectedIds[0], byRowId[0][0].toString().toInteger())

        def combinedPredicate = sql("""
            select id
            from ${tableName}
            where id >= ${expectedIds[1]} and _row_id in (${rowLineageRows[1][1]}, ${rowLineageRows[2][1]})
            order by id
        """)
        log.info("Checking combined business + _row_id predicate for ${tableName}: result=${combinedPredicate}")
        assertEquals(2, combinedPredicate.size())
        assertEquals(expectedIds[1], combinedPredicate[0][0].toString().toInteger())
        assertEquals(expectedIds[2], combinedPredicate[1][0].toString().toInteger())
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
        formats.each { format ->
            String unpartitionedTable = "test_row_lineage_query_insert_unpartitioned_${format}"
            String partitionedTable = "test_row_lineage_query_insert_partitioned_${format}"
            log.info("Run row lineage query/insert test with format ${format}")

            try {
                sql """drop table if exists ${unpartitionedTable}"""
                sql """
                    create table ${unpartitionedTable} (
                        id int,
                        name string,
                        age int
                    ) engine=iceberg
                    properties (
                        "format-version" = "3",
                        "write.format.default" = "${format}"
                    )
                """

                sql """
                    insert into ${unpartitionedTable} values(1, 'Alice', 25);
                """
                sql """ insert into ${unpartitionedTable} values(2, 'Bob', 30) """
                sql """ insert into ${unpartitionedTable} values(3, 'Charlie', 35) """

                log.info("Inserted initial rows into ${unpartitionedTable}")

                // Assert baseline:
                // 1. DESC and SELECT * hide row lineage columns by default.
                // 2. show_hidden_columns=true exposes both hidden columns in DESC and SELECT *.
                // 3. Explicit SELECT on row lineage columns returns non-null values.
                assertRowLineageHiddenColumns(unpartitionedTable, 3)
                assertExplicitRowLineageReadable(unpartitionedTable, [1, 2, 3])

                test {
                    sql """insert into ${unpartitionedTable}(_row_id, id, name, age) values (1, 9, 'BadRow', 99)"""
                    exception "Cannot specify row lineage column '_row_id' in INSERT statement"
                }

                test {
                    sql """
                        insert into ${unpartitionedTable}(_last_updated_sequence_number, id, name, age)
                        values (1, 10, 'BadSeq', 100)
                    """
                    exception "Cannot specify row lineage column '_last_updated_sequence_number' in INSERT statement"
                }

                sql """insert into ${unpartitionedTable}(id, name, age) values (4, 'Doris', 40)"""
                def unpartitionedCount = sql """select count(*) from ${unpartitionedTable}"""
                log.info("Checking row count after regular INSERT for ${unpartitionedTable}: result=${unpartitionedCount}")
                assertEquals(4, unpartitionedCount[0][0].toString().toInteger())

                assertCurrentFilesDoNotContainRowLineageColumns(
                        unpartitionedTable,
                        format,
                        "Unpartitioned normal INSERT")

                sql """drop table if exists ${partitionedTable}"""
                sql """
                    create table ${partitionedTable} (
                        id int,
                        name string,
                        age int,
                        dt date
                    ) engine=iceberg
                    partition by list (day(dt)) ()
                    properties (
                        "format-version" = "3",
                        "write.format.default" = "${format}"
                    )
                """

                sql """ insert into ${partitionedTable} values(11, 'Penny', 21, '2024-01-01')"""
                sql """ insert into ${partitionedTable} values(12, 'Quinn', 22, '2024-01-02')"""
                sql """ insert into ${partitionedTable} values(13, 'Rita', 23, '2024-01-03')"""        
                
                log.info("Inserted initial rows into ${partitionedTable}")

                // Assert baseline:
                // 1. Partitioned tables follow the same row lineage semantics as unpartitioned tables.
                // 2. Explicit SELECT on _row_id remains readable under partition predicates.
                // 3. Regular INSERT still rejects hidden columns and does not write them physically.
                assertRowLineageHiddenColumns(partitionedTable, 4)

                def partitionLineageRows = sql """
                    select id, _row_id, _last_updated_sequence_number
                    from ${partitionedTable}
                    where dt >= '2024-01-01'
                    order by id
                """
                log.info("Checking partitioned row lineage projection for ${partitionedTable}: rows=${partitionLineageRows}")
                assertEquals(3, partitionLineageRows.size())
                partitionLineageRows.each { row ->
                    assertTrue(row[1] != null, "_row_id should be non-null for partitioned table row=${row}")
                    assertTrue(row[2] != null, "_last_updated_sequence_number should be non-null for partitioned table row=${row}")
                }

                def exactPartitionPredicate = sql """
                    select id
                    from ${partitionedTable}
                    where dt = '2024-01-02' and _row_id = ${partitionLineageRows[1][1]}
                """
                log.info("Checking exact partition + _row_id predicate for ${partitionedTable}: result=${exactPartitionPredicate}")
                assertEquals(1, exactPartitionPredicate.size())
                assertEquals(12, exactPartitionPredicate[0][0].toString().toInteger())

                test {
                    sql """
                        insert into ${partitionedTable}(_row_id, id, name, age, dt)
                        values (1, 14, 'BadPartitionRow', 24, '2024-01-04')
                    """
                    exception "Cannot specify row lineage column '_row_id' in INSERT statement"
                }

                test {
                    sql """
                        insert into ${partitionedTable}(_last_updated_sequence_number, id, name, age, dt)
                        values (1, 15, 'BadPartitionSeq', 25, '2024-01-05')
                    """
                    exception "Cannot specify row lineage column '_last_updated_sequence_number' in INSERT statement"
                }

                sql """insert into ${partitionedTable}(id, name, age, dt) values (14, 'Sara', 24, '2024-01-04')"""
                def partitionedCount = sql """select count(*) from ${partitionedTable}"""
                log.info("Checking row count after regular INSERT for ${partitionedTable}: result=${partitionedCount}")
                assertEquals(4, partitionedCount[0][0].toString().toInteger())

                assertCurrentFilesDoNotContainRowLineageColumns(
                        partitionedTable,
                        format,
                        "Partitioned normal INSERT")
            } finally {
                sql """drop table if exists ${partitionedTable}"""
                sql """drop table if exists ${unpartitionedTable}"""
            }
        }
    } finally {
        sql """set show_hidden_columns = false"""
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
