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

suite("test_iceberg_v3_row_lineage_continuous_dml", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_continuous_dml"
    String dbName = "test_row_lineage_continuous_dml_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def partitionFlags = [true, false]

    def lineageMap = { tableName ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        Map<Integer, List<Long>> result = [:]
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for ${tableName}, row=${row}")
            assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName}, row=${row}")
            result[row[0].toString().toInteger()] = [row[1].toString().toLong(), row[2].toString().toLong()]
        }
        return result
    }

    def assertDeleteFilesArePuffin = { tableName ->
        def deleteFiles = sql("""
            select file_path, lower(file_format), record_count
            from ${tableName}\$delete_files
            order by file_path
        """)
        log.info("Checking continuous DML delete files for ${tableName}: ${deleteFiles}")
        assertTrue(deleteFiles.size() > 0, "Continuous MOR DML should create delete files for ${tableName}")
        deleteFiles.each { row ->
            assertTrue(row[0].toString().toLowerCase().endsWith(".puffin"),
                    "v3 delete file should be Puffin for ${tableName}, row=${row}")
            assertEquals("puffin", row[1].toString())
        }
    }

    def assertDeleteFilesDescCaptured = { tableName ->
        def descRows = sql("""desc ${tableName}\$delete_files""")
        log.info("DESC ${tableName}\$delete_files: ${descRows}")
        assertTrue(descRows.size() > 0, "delete_files system table schema should be visible for ${tableName}")
    }

    def assertVisibleRowsAfterV2PositionDeleteMerge = { tableName ->
        def rows = sql("""select id, name, score, dt from ${tableName} order by id""")
        log.info("Rows after v2 position delete to v3 DV merge for ${tableName}: ${rows}")
        assertEquals(1, rows.size())
        assertEquals(3, rows[0][0].toString().toInteger())
        assertEquals("c", rows[0][1].toString())
        assertEquals(30, rows[0][2].toString().toInteger())
        assertEquals("2024-06-02", rows[0][3].toString())

        def resurrectedRows = sql("""select count(*) from ${tableName} where id in (1, 2)""")
        assertEquals(0, resurrectedRows[0][0].toString().toInteger())
    }

    def hasSparkIcebergJdbc = {
        try {
            spark_iceberg_jdbc """select 1"""
            return true
        } catch (Exception e) {
            logger.info("Check spark-iceberg JDBC failed: ${e.message}")
            return false
        }
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
        boolean sparkIcebergAvailable = hasSparkIcebergJdbc()
        if (sparkIcebergAvailable) {
            spark_iceberg_jdbc """create database if not exists demo.${dbName}"""
        } else {
            logger.info("spark-iceberg JDBC is unavailable, skip v2 position delete to v3 DV merge branch")
        }
        formats.each { format ->
            partitionFlags.each { partitioned ->
                String partitionSuffix = partitioned ? "part" : "unpart"
                String tableName = "continuous_dml_${format}_${partitionSuffix}"
                String partitionClause = partitioned ? "partition by list (day(dt)) ()" : ""
                try {
                    sql """drop table if exists ${tableName}"""
                    sql """
                        create table ${tableName} (
                            id int,
                            name string,
                            score int,
                            dt date
                        ) engine=iceberg
                        ${partitionClause}
                        properties (
                            "format-version" = "3",
                            "write.format.default" = "${format}",
                            "write.delete.mode" = "merge-on-read",
                            "write.update.mode" = "merge-on-read",
                            "write.merge.mode" = "merge-on-read"
                        )
                    """
                    sql """
                        insert into ${tableName} values
                        (1, 'a', 10, date '2024-06-01'),
                        (2, 'b', 20, date '2024-06-01'),
                        (3, 'c', 30, date '2024-06-01'),
                        (4, 'd', 40, date '2024-06-01'),
                        (5, 'e', 50, date '2024-06-01')
                    """

                    Map<Integer, List<Long>> beforeLineage = lineageMap(tableName)
                    sql """delete from ${tableName} where id = 1"""
                    sql """update ${tableName} set score = score + 100 where id = 2"""
                    Map<Integer, List<Long>> afterUpdateLineage = lineageMap(tableName)
                    assertEquals(beforeLineage[2][0], afterUpdateLineage[2][0])
                    assertTrue(afterUpdateLineage[2][1] > beforeLineage[2][1],
                            "UPDATE should advance sequence for id=2 in ${tableName}")

                    sql """
                        merge into ${tableName} t
                        using (
                            select 2 as id, 'b_m' as name, 222 as score, date '2024-06-01' as dt, 'U' as flag
                            union all
                            select 3, 'c', 30, date '2024-06-01', 'D'
                            union all
                            select 6, 'f', 60, date '2024-06-01', 'I'
                        ) s
                        on t.id = s.id
                        when matched and s.flag = 'D' then delete
                        when matched then update set name = s.name, score = s.score
                        when not matched then insert (id, name, score, dt)
                        values (s.id, s.name, s.score, s.dt)
                    """

                    def businessRows = sql("""select id, name, score, dt from ${tableName} order by id""")
                    def normalizedRows = businessRows.collect {
                        [it[0].toString().toInteger(), it[1].toString(), it[2].toString().toInteger(), it[3].toString()]
                    }
                    assertEquals([
                            [2, "b_m", 222, "2024-06-01"],
                            [4, "d", 40, "2024-06-01"],
                            [5, "e", 50, "2024-06-01"],
                            [6, "f", 60, "2024-06-01"]
                    ], normalizedRows)

                    Map<Integer, List<Long>> afterMergeLineage = lineageMap(tableName)
                    assertEquals(afterUpdateLineage[2][0], afterMergeLineage[2][0])
                    assertTrue(afterMergeLineage[2][1] > afterUpdateLineage[2][1],
                            "MERGE UPDATE should advance sequence for id=2 in ${tableName}")
                    assertEquals(beforeLineage[4], afterMergeLineage[4])
                    assertEquals(beforeLineage[5], afterMergeLineage[5])
                    assertTrue(!afterMergeLineage.containsKey(1), "id=1 should remain deleted in ${tableName}")
                    assertTrue(!afterMergeLineage.containsKey(3), "id=3 should be deleted by MERGE in ${tableName}")
                    assertTrue(afterMergeLineage[6][0] != null)

                    assertDeleteFilesDescCaptured(tableName)
                    assertDeleteFilesArePuffin(tableName)

                    def snapshots = sql("""select snapshot_id, operation from ${tableName}\$snapshots order by committed_at""")
                    log.info("Continuous DML snapshots for ${tableName}: ${snapshots}")
                    assertTrue(snapshots.size() >= 4, "Continuous DML should create snapshots for ${tableName}")
                } finally {
                    sql """drop table if exists ${tableName}"""
                }
            }

            if (sparkIcebergAvailable) {
                String v2PositionDeleteTable = "v2_pos_to_v3_dv_${format}"
                try {
                    spark_iceberg_jdbc_multi """
                        drop table if exists demo.${dbName}.${v2PositionDeleteTable};
                        create table demo.${dbName}.${v2PositionDeleteTable} (
                            id int,
                            name string,
                            score int,
                            dt date
                        ) using iceberg
                        partitioned by (days(dt))
                        tblproperties (
                            'format-version' = '2',
                            'write.format.default' = '${format}',
                            'write.delete.mode' = 'merge-on-read',
                            'write.update.mode' = 'merge-on-read',
                            'write.merge.mode' = 'merge-on-read'
                        );
                        insert into demo.${dbName}.${v2PositionDeleteTable} values
                        (1, 'a', 10, date '2024-06-02'),
                        (2, 'b', 20, date '2024-06-02'),
                        (3, 'c', 30, date '2024-06-02');
                        delete from demo.${dbName}.${v2PositionDeleteTable} where id = 1;
                        alter table demo.${dbName}.${v2PositionDeleteTable}
                        set tblproperties ('format-version' = '3');
                    """

                    sql """refresh table ${dbName}.${v2PositionDeleteTable}"""
                    def baselineRows = sql("""select id, name, score, dt from ${v2PositionDeleteTable} order by id""")
                    assertEquals(2, baselineRows.size())
                    assertEquals(2, baselineRows[0][0].toString().toInteger())
                    assertEquals(3, baselineRows[1][0].toString().toInteger())

                    sql """delete from ${v2PositionDeleteTable} where id = 2"""
                    assertVisibleRowsAfterV2PositionDeleteMerge(v2PositionDeleteTable)
                    assertDeleteFilesDescCaptured(v2PositionDeleteTable)

                    def deleteFiles = sql("""
                        select file_path, lower(file_format), record_count
                        from ${v2PositionDeleteTable}\$delete_files
                        order by file_path
                    """)
                    log.info("Delete files after v2 position delete to v3 DV merge for ${v2PositionDeleteTable}: ${deleteFiles}")
                    assertTrue(deleteFiles.any { row -> row[1].toString() == "puffin" },
                            "Doris v3 DELETE should create a Puffin DV for ${v2PositionDeleteTable}")
                    deleteFiles.each { row ->
                        assertEquals("puffin", row[1].toString())
                        assertTrue(row[0].toString().toLowerCase().endsWith(".puffin"),
                                "live delete files should be rewritten to Puffin DV after v2 position delete merge: ${row}")
                    }
                } finally {
                    sql """drop table if exists ${v2PositionDeleteTable}"""
                }
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
