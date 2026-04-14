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

suite("test_iceberg_v3_row_lineage_update_delete_merge", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_update_delete_merge"
    String dbName = "test_row_lineage_update_delete_merge_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]

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

    def assertDeleteFilesArePuffin = { tableName ->
        def deleteFiles = sql("""
            select file_path, lower(file_format)
            from ${tableName}\$delete_files
            order by file_path
        """)
        log.info("Checking delete files for ${tableName}: ${deleteFiles}")
        assertTrue(deleteFiles.size() > 0, "V3 table ${tableName} should produce delete files")
        deleteFiles.each { row ->
            assertTrue(row[0].toString().endsWith(".puffin"),
                    "V3 delete file should be Puffin: ${row}")
            assertEquals("puffin", row[1].toString())
        }
    }

    def assertAtLeastOneCurrentDataFileHasRowLineageColumns = { tableName, format ->
        def currentFiles = sql("""select file_path, lower(file_format) from ${tableName}\$data_files order by file_path""")
        log.info("Checking current data files for physical row lineage columns in ${tableName}: ${currentFiles}")
        assertTrue(currentFiles.size() > 0, "Current data files should exist for ${tableName}")

        boolean found = false
        currentFiles.each { row ->
            assertEquals(format, row[1].toString())
            assertTrue(row[0].toString().endsWith(format == "parquet" ? ".parquet" : ".orc"),
                    "Current data file should match ${format} for ${tableName}, file=${row[0]}")
            def schemaRows = fileSchemaRows(row[0].toString(), format)
            log.info("${format} schema for ${tableName}, file=${row[0]} -> ${schemaRows}")
            if (schemaContainsField(schemaRows, "_row_id")
                    && schemaContainsField(schemaRows, "_last_updated_sequence_number")) {
                found = true
            }
        }
        assertTrue(found, "At least one current data file should physically contain row lineage columns for ${tableName}")
    }

    def assertExplicitRowLineageNonNull = { tableName, expectedRowCount ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        log.info("Checking explicit row lineage projection for ${tableName}: rows=${rows}")
        assertEquals(expectedRowCount, rows.size())
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for ${tableName}, row=${row}")
            assertTrue(row[2] != null, "_last_updated_sequence_number should be non-null for ${tableName}, row=${row}")
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
            String updateDeleteTable = "test_row_lineage_v3_update_delete_${format}"
            String mergeTable = "test_row_lineage_v3_merge_${format}"
            log.info("Run row lineage update/delete/merge test with format ${format}")

            try {
                sql """drop table if exists ${updateDeleteTable}"""
                sql """
                    create table ${updateDeleteTable} (
                        id int,
                        name string,
                        age int
                    ) engine=iceberg
                    properties (
                        "format-version" = "3",
                        "write.format.default" = "${format}"
                    )
                """

                sql """insert into ${updateDeleteTable} values (1, 'Alice', 25) """ 
                sql """insert into ${updateDeleteTable} values (2, 'Bob', 30) """ 
                sql """insert into ${updateDeleteTable} values (3, 'Charlie', 35)""" 

                def updateDeleteLineageBefore = lineageMap(updateDeleteTable)
                log.info("Lineage before UPDATE/DELETE on ${updateDeleteTable}: ${updateDeleteLineageBefore}")
                sql """update ${updateDeleteTable} set name = 'Alice_u', age = 26 where id = 1"""
                sql """delete from ${updateDeleteTable} where id = 2"""

                // Assert baseline:
                // 1. UPDATE keeps rows readable and applies the new values.
                // 2. DELETE removes the target row.
                // 3. V3 delete files use Puffin deletion vectors instead of delete_pos parquet/orc files.
                // 4. Explicit row lineage reads remain non-null after DML.
                def updateDeleteRows = sql """select * from ${updateDeleteTable} order by id"""
                log.info("Checking table rows after UPDATE/DELETE on ${updateDeleteTable}: ${updateDeleteRows}")
                assertEquals(2, updateDeleteRows.size())
                assertEquals(1, updateDeleteRows[0][0].toString().toInteger())
                assertEquals("Alice_u", updateDeleteRows[0][1])
                assertEquals(26, updateDeleteRows[0][2].toString().toInteger())
                assertEquals(3, updateDeleteRows[1][0].toString().toInteger())
                assertEquals("Charlie", updateDeleteRows[1][1])
                assertEquals(35, updateDeleteRows[1][2].toString().toInteger())

                assertExplicitRowLineageNonNull(updateDeleteTable, 2)
                def updateDeleteLineageAfter = lineageMap(updateDeleteTable)
                log.info("Lineage after UPDATE/DELETE on ${updateDeleteTable}: ${updateDeleteLineageAfter}")
                assertEquals(updateDeleteLineageBefore[1][0], updateDeleteLineageAfter[1][0])
                assertTrue(updateDeleteLineageBefore[1][1] != updateDeleteLineageAfter[1][1],
                        "UPDATE should change _last_updated_sequence_number for id=1")
                assertTrue(updateDeleteLineageAfter[1][1].toLong() > updateDeleteLineageBefore[1][1].toLong(),
                        "UPDATE should advance _last_updated_sequence_number for id=1")
                assertEquals(updateDeleteLineageBefore[3][0], updateDeleteLineageAfter[3][0])
                assertEquals(updateDeleteLineageBefore[3][1], updateDeleteLineageAfter[3][1])
                assertTrue(!updateDeleteLineageAfter.containsKey(2), "Deleted row id=2 should not remain after DELETE")
                assertDeleteFilesArePuffin(updateDeleteTable)
                assertAtLeastOneCurrentDataFileHasRowLineageColumns(updateDeleteTable, format)

                def minRowIdAfterUpdate = sql """
                    select min(_row_id)
                    from ${updateDeleteTable}
                """
                def rowIdFilterResult = sql """
                    select count(*)
                    from ${updateDeleteTable}
                    where _row_id = ${minRowIdAfterUpdate[0][0]}
                """
                log.info("Checking _row_id filter after UPDATE/DELETE on ${updateDeleteTable}: minRowId=${minRowIdAfterUpdate}, result=${rowIdFilterResult}")
                assertEquals(1, rowIdFilterResult[0][0].toString().toInteger())

                sql """drop table if exists ${mergeTable}"""
                sql """
                    create table ${mergeTable} (
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

                sql """ insert into ${mergeTable} values (1, 'Penny', 21, '2024-01-01') """
                sql """ insert into ${mergeTable} values (2, 'Quinn', 22, '2024-01-02') """
                sql """ insert into ${mergeTable} values (3, 'Rita', 23, '2024-01-03') """

                def mergeLineageBefore = lineageMap(mergeTable)
                log.info("Lineage before MERGE on ${mergeTable}: ${mergeLineageBefore}")
                sql """
                    merge into ${mergeTable} t
                    using (
                        select 1 as id, 'Penny_u' as name, 31 as age, date '2024-01-01' as dt, 'U' as flag
                        union all
                        select 2, 'Quinn', 22, date '2024-01-02', 'D'
                        union all
                        select 4, 'Sara', 24, date '2024-01-04', 'I'
                    ) s
                    on t.id = s.id
                    when matched and s.flag = 'D' then delete
                    when matched then update set
                        name = s.name,
                        age = s.age
                    when not matched then insert (id, name, age, dt)
                    values (s.id, s.name, s.age, s.dt)
                """

                // Assert baseline:
                // 1. MERGE applies DELETE, UPDATE, and INSERT actions in one statement.
                // 2. The partitioned MERGE still writes Puffin deletion vectors.
                // 3. At least one current data file written by MERGE contains physical row lineage columns.
                def mergeRows = sql """select * from ${mergeTable} order by id"""
                log.info("Checking table rows after MERGE on ${mergeTable}: ${mergeRows}")
                assertEquals(3, mergeRows.size())
                assertEquals(1, mergeRows[0][0].toString().toInteger())
                assertEquals("Penny_u", mergeRows[0][1])
                assertEquals(31, mergeRows[0][2].toString().toInteger())
                assertEquals(3, mergeRows[1][0].toString().toInteger())
                assertEquals("Rita", mergeRows[1][1])
                assertEquals(23, mergeRows[1][2].toString().toInteger())
                assertEquals(4, mergeRows[2][0].toString().toInteger())
                assertEquals("Sara", mergeRows[2][1])
                assertEquals(24, mergeRows[2][2].toString().toInteger())

                assertExplicitRowLineageNonNull(mergeTable, 3)
                def mergeLineageAfter = lineageMap(mergeTable)
                log.info("Lineage after MERGE on ${mergeTable}: ${mergeLineageAfter}")
                assertEquals(mergeLineageBefore[1][0], mergeLineageAfter[1][0])
                assertTrue(mergeLineageBefore[1][1] != mergeLineageAfter[1][1],
                        "MERGE UPDATE should change _last_updated_sequence_number for id=1")
                assertTrue(mergeLineageAfter[1][1].toLong() > mergeLineageBefore[1][1].toLong(),
                        "MERGE UPDATE should advance _last_updated_sequence_number for id=1")
                assertEquals(mergeLineageBefore[3][0], mergeLineageAfter[3][0])
                assertEquals(mergeLineageBefore[3][1], mergeLineageAfter[3][1])
                assertTrue(!mergeLineageAfter.containsKey(2), "MERGE DELETE should remove id=2")
                assertDeleteFilesArePuffin(mergeTable)
                assertAtLeastOneCurrentDataFileHasRowLineageColumns(mergeTable, format)

                def insertedRowLineage = sql """
                    select _row_id, _last_updated_sequence_number
                    from ${mergeTable}
                    where id = 4
                """
                log.info("Checking inserted MERGE row lineage for ${mergeTable}: ${insertedRowLineage}")
                assertEquals(1, insertedRowLineage.size())
                assertTrue(insertedRowLineage[0][0] != null, "Inserted MERGE row should get generated _row_id")
                assertTrue(insertedRowLineage[0][1] != null, "Inserted MERGE row should get generated _last_updated_sequence_number")
            } finally {
                sql """drop table if exists ${mergeTable}"""
                sql """drop table if exists ${updateDeleteTable}"""
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
