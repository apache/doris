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

suite("test_iceberg_v3_row_lineage_schema_evolution", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_schema_evolution"
    String dbName = "test_row_lineage_schema_evolution_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def partitionFlags = [true, false]

    def descColumns = { tableName ->
        return sql("""desc ${tableName}""").collect { row -> row[0].toString().toLowerCase() }
    }

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
            logger.info("spark-iceberg JDBC is unavailable, skip Spark schema evolution branch")
        }
        formats.each { format ->
            partitionFlags.each { partitioned ->
                String partitionSuffix = partitioned ? "part" : "unpart"
                String tableName = "schema_evo_${format}_${partitionSuffix}"
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
                            "write.update.mode" = "merge-on-read",
                            "write.merge.mode" = "merge-on-read"
                        )
                    """
                    sql """
                        insert into ${tableName} values
                        (1, 'a', 10, date '2024-10-01'),
                        (2, 'b', 20, date '2024-10-01')
                    """

                    Map<Integer, List<Long>> beforeLineage = lineageMap(tableName)
                    def initialColumns = descColumns(tableName)
                    assertTrue(!initialColumns.contains("_row_id"))
                    assertTrue(!initialColumns.contains("_last_updated_sequence_number"))

                    sql """alter table ${tableName} add column extra string"""
                    sql """refresh table ${dbName}.${tableName}"""
                    def addColumns = descColumns(tableName)
                    assertTrue(addColumns.contains("extra"), "ADD COLUMN should expose extra for ${tableName}")

                    def afterAddRows = sql("""
                        select id, name, score, extra, dt, _row_id, _last_updated_sequence_number
                        from ${tableName}
                        order by id
                    """)
                    log.info("Rows after ADD COLUMN for ${tableName}: ${afterAddRows}")
                    assertEquals(2, afterAddRows.size())
                    afterAddRows.each { row ->
                        assertTrue(row[3] == null, "New extra column should be null for old rows in ${tableName}")
                        assertTrue(row[5] != null, "_row_id should remain readable after ADD COLUMN in ${tableName}")
                        assertTrue(row[6] != null,
                                "_last_updated_sequence_number should remain readable after ADD COLUMN in ${tableName}")
                    }

                    sql """alter table ${tableName} drop column extra"""
                    sql """refresh table ${dbName}.${tableName}"""
                    def dropColumns = descColumns(tableName)
                    assertTrue(!dropColumns.contains("extra"), "DROP COLUMN should remove extra for ${tableName}")

                    def afterDropRows = sql("""
                        select id, name, score, dt, _row_id, _last_updated_sequence_number
                        from ${tableName}
                        order by id
                    """)
                    log.info("Rows after DROP COLUMN for ${tableName}: ${afterDropRows}")
                    assertEquals(2, afterDropRows.size())
                    assertEquals("a", afterDropRows[0][1].toString())
                    assertEquals(10, afterDropRows[0][2].toString().toInteger())
                    assertEquals("2024-10-01", afterDropRows[0][3].toString())

                    sql """update ${tableName} set score = score + 100 where id = 1"""
                    Map<Integer, List<Long>> afterUpdateLineage = lineageMap(tableName)
                    assertEquals(beforeLineage[1][0], afterUpdateLineage[1][0])
                    assertTrue(afterUpdateLineage[1][1] > beforeLineage[1][1],
                            "UPDATE after schema evolution should advance sequence for id=1 in ${tableName}")
                    assertEquals(beforeLineage[2], afterUpdateLineage[2])

                    def finalRows = sql("""
                        select id, name, score, dt, _row_id, _last_updated_sequence_number
                        from ${tableName}
                        order by id
                    """)
                    assertEquals(110, finalRows[0][2].toString().toInteger())
                    assertEquals(20, finalRows[1][2].toString().toInteger())

                    if (sparkIcebergAvailable) {
                        spark_iceberg_jdbc """
                            alter table demo.${dbName}.${tableName}
                            add columns (spark_added string)
                        """
                        sql """refresh table ${dbName}.${tableName}"""
                        def sparkAddColumns = descColumns(tableName)
                        assertTrue(sparkAddColumns.contains("spark_added"),
                                "Spark ADD COLUMN should be visible after Doris REFRESH TABLE for ${tableName}")

                        def afterSparkAddRows = sql("""
                            select id, name, score, spark_added, dt, _row_id, _last_updated_sequence_number
                            from ${tableName}
                            order by id
                        """)
                        log.info("Rows after Spark ADD COLUMN for ${tableName}: ${afterSparkAddRows}")
                        assertEquals(2, afterSparkAddRows.size())
                        afterSparkAddRows.each { row ->
                            assertTrue(row[3] == null,
                                    "Spark-added column should be null for existing rows in ${tableName}")
                            assertTrue(row[5] != null, "_row_id should remain readable after Spark schema evolution")
                            assertTrue(row[6] != null,
                                    "_last_updated_sequence_number should remain readable after Spark schema evolution")
                        }
                    }
                } finally {
                    sql """drop table if exists ${tableName}"""
                }
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
