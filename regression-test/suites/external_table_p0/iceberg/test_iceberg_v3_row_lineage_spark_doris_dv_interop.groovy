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

suite("test_iceberg_v3_row_lineage_spark_doris_dv_interop", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_spark_doris_dv_interop"
    String dbName = "test_row_lineage_spark_doris_dv_interop_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def partitionFlags = [true, false]

    def normalizeRows = { rows ->
        return rows.collect { row -> row.collect { col -> col == null ? null : col.toString() } }
    }

    def assertSparkDorisBusinessRows = { tableName ->
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        sql """refresh table ${dbName}.${tableName}"""
        def sparkRows = spark_iceberg("""
            select id, name, score, dt
            from demo.${dbName}.${tableName}
            order by id
        """)
        def dorisRows = sql("""
            select id, name, score, dt
            from ${tableName}
            order by id
        """)
        log.info("Spark business rows for ${tableName}: ${sparkRows}")
        log.info("Doris business rows for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    def assertDorisBusinessRows = { tableName, expectedRows ->
        def dorisRows = sql("""
            select id, name, score, dt
            from ${tableName}
            order by id
        """)
        def normalizedRows = normalizeRows(dorisRows)
        log.info("Checking concrete Doris business rows for ${tableName}: ${normalizedRows}")
        assertEquals(expectedRows, normalizedRows)
    }

    def assertSparkDorisAggregateRows = { tableName ->
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        sql """refresh table ${dbName}.${tableName}"""
        def sparkRows = spark_iceberg("""
            select count(*), sum(id), sum(score)
            from demo.${dbName}.${tableName}
        """)
        def dorisRows = sql("""
            select count(*), sum(id), sum(score)
            from ${tableName}
        """)
        log.info("Spark aggregate rows for ${tableName}: ${sparkRows}")
        log.info("Doris aggregate rows for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    def assertDorisAggregateRows = { tableName, expectedRows ->
        def dorisRows = sql("""
            select count(*), sum(id), sum(score)
            from ${tableName}
        """)
        def normalizedRows = normalizeRows(dorisRows)
        log.info("Checking concrete Doris aggregate rows for ${tableName}: ${normalizedRows}")
        assertEquals(expectedRows, normalizedRows)
    }

    def assertDorisLineageReadable = { tableName ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """)
        log.info("Checking row lineage in ${tableName}: ${rows}")
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for ${tableName}, row=${row}")
            assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName}, row=${row}")
        }
    }

    def deleteFiles = { tableName ->
        return sql("""
            select file_path, lower(file_format)
            from ${tableName}\$delete_files
            order by file_path
        """)
    }

    def assertPuffinDeleteFiles = { tableName ->
        def rows = deleteFiles(tableName)

        log.info("Checking Puffin delete files" +
            " for ${tableName}: ${rows}")
        assertTrue(rows.size() > 0, "v3 delete should create delete files for ${tableName}")
        rows.each { row ->
            assertEquals("puffin", row[1].toString())
            assertTrue(row[0].toString().toLowerCase().endsWith(".puffin"))
        }
    }

    def assertSparkGeneratedDvReadable = {
        spark_iceberg """refresh table demo.format_v3.dv_test"""
        sql """refresh table format_v3.dv_test"""
        assertPuffinDeleteFiles("format_v3.dv_test")
        def sparkRows = spark_iceberg("""
            select id
            from demo.format_v3.dv_test
            order by id
        """)
        def dorisRows = sql("""
            select id
            from format_v3.dv_test
            order by id
        """)
        log.info("Checking Spark/Doris read result for Spark-generated DV fixture format_v3.dv_test: " +
            "Spark=${sparkRows}, Doris=${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)

        def rows = sql("""select id, _row_id, _last_updated_sequence_number from format_v3.dv_test order by id""")
        log.info("Checking Doris row lineage for Spark-generated DV fixture format_v3.dv_test: ${rows}")
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for Spark DV fixture, row=${row}")
            assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null for Spark DV fixture, row=${row}")
        }
        def sparkChecksumRows = spark_iceberg("""
            select count(*), sum(id)
            from demo.format_v3.dv_test
        """)
        def dorisChecksumRows = sql("""
            select count(*), sum(id)
            from format_v3.dv_test
        """)
        assertSparkDorisResultEquals(sparkChecksumRows, dorisChecksumRows)
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
        assertSparkGeneratedDvReadable()
        spark_iceberg """create database if not exists demo.${dbName}"""
        formats.each { format ->
            partitionFlags.each { partitioned ->
                String partitionSuffix = partitioned ? "part" : "unpart"
                String sparkDvTable = "spark_dv_${format}_${partitionSuffix}"
                String dorisDvTable = "doris_dv_${format}_${partitionSuffix}"
                String sparkPartitionClause = partitioned ? "partitioned by (days(dt))" : ""
                String dorisPartitionClause = partitioned ? "partition by list (day(dt)) ()" : ""

                try {
                    spark_iceberg_multi """
                        drop table if exists demo.${dbName}.${sparkDvTable};
                        create table demo.${dbName}.${sparkDvTable} (
                            id int,
                            name string,
                            score int,
                            dt date
                        ) using iceberg
                        ${sparkPartitionClause}
                        tblproperties (
                            'format-version' = '3',
                            'write.format.default' = '${format}',
                            'write.delete.mode' = 'merge-on-read',
                            'write.update.mode' = 'merge-on-read',
                            'write.merge.mode' = 'merge-on-read',
                            'write.distribution-mode' = 'none',
                            'write.delete.granularity' = 'file'
                        );
                        set spark.sql.shuffle.partitions = 1;
                        set spark.sql.adaptive.enabled = false;
                        insert into demo.${dbName}.${sparkDvTable}
                        select /*+ REPARTITION(1) */
                            id,
                            name,
                            score,
                            dt
                        from values
                            (1, 'a', 10, date '2024-09-01'),
                            (2, 'b', 20, date '2024-09-01'),
                            (3, 'c', 30, date '2024-09-02'),
                            (4, 'd', 40, date '2024-09-02')
                        as t(id, name, score, dt);
                        delete from demo.${dbName}.${sparkDvTable} where id in (2, 4);
                        reset spark.sql.shuffle.partitions;
                        reset spark.sql.adaptive.enabled;
                    """
                    assertSparkDorisBusinessRows(sparkDvTable)
                    assertDorisLineageReadable(sparkDvTable)
                    assertPuffinDeleteFiles(sparkDvTable)
                    assertSparkDorisAggregateRows(sparkDvTable)

                    sql """drop table if exists ${dorisDvTable}"""
                    sql """
                        create table ${dorisDvTable} (
                            id int,
                            name string,
                            score int,
                            dt date
                        ) engine=iceberg
                        ${dorisPartitionClause}
                        properties (
                            "format-version" = "3",
                            "write.format.default" = "${format}",
                            "write.delete.mode" = "merge-on-read"
                        )
                    """
                    sql """
                        insert into ${dorisDvTable} values
                        (1, 'a', 10, date '2024-09-01'),
                        (2, 'b', 20, date '2024-09-01'),
                        (3, 'c', 30, date '2024-09-02')
                    """
                    sql """delete from ${dorisDvTable} where id = 2"""
                    assertSparkDorisBusinessRows(dorisDvTable)
                    assertDorisBusinessRows(dorisDvTable, [
                            ["1", "a", "10", "2024-09-01"],
                            ["3", "c", "30", "2024-09-02"]
                    ])
                    assertDorisLineageReadable(dorisDvTable)
                    assertPuffinDeleteFiles(dorisDvTable)
                    assertSparkDorisAggregateRows(dorisDvTable)
                    assertDorisAggregateRows(dorisDvTable, [["2", "4", "40"]])
                } finally {
                    sql """drop table if exists ${dorisDvTable}"""
                    sql """drop table if exists ${sparkDvTable}"""
                }
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
