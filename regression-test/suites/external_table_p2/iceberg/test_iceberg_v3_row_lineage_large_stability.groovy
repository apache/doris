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

suite("test_iceberg_v3_row_lineage_large_stability", "p2,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }


    String catalogName = "test_iceberg_v3_row_lineage_large_stability"
    String dbName = "test_row_lineage_large_stability_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    int rowCount = (context.config.otherConfigs.get("icebergRowLineageLargeRows") ?: "500000").toInteger()
    def formats = ["parquet", "orc"]
    def dataColumnNames = (1..498).collect { idx -> String.format("c%03d", idx) }

    def assertChecksum = { tableName, expectedRows ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkRows = spark_iceberg("""
            select count(*), sum(id), sum(c001)
            from demo.${dbName}.${tableName}
        """)
        def dorisRows = sql("""
            select count(*), sum(id), sum(c001)
            from ${tableName}
        """)
        log.info("Spark checksum for ${tableName}: ${sparkRows}")
        log.info("Doris checksum for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
        assertEquals(expectedRows, dorisRows[0][0].toString().toLong())
        assertTrue(dorisRows[0][1] != null, "sum(id) should be non-null for ${tableName}")
        assertTrue(dorisRows[0][2] != null, "sum(c001) should be non-null for ${tableName}")
        return dorisRows[0].collect { it == null ? null : it.toString() }
    }

    def assertPuffinDeleteFiles = { tableName ->
        def deleteFiles = sql("""
            select file_path, lower(file_format), record_count
            from ${tableName}\$delete_files
            order by file_path
        """)
        log.info("Large stability delete files for ${tableName}: ${deleteFiles}")
        assertTrue(deleteFiles.size() > 0, "Large stability DML should create delete files for ${tableName}")
        deleteFiles.each { row ->
            assertEquals("puffin", row[1].toString())
            assertTrue(row[0].toString().toLowerCase().endsWith(".puffin"))
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
        formats.each { format ->
            String tableName = "large_stability_${format}"
            try {
                sql """drop table if exists ${tableName}"""
                String dataColumns = dataColumnNames.collect { col -> "${col} int" }.join(",\n                        ")
                String dataSelectExprs = dataColumnNames.withIndex().collect { col, idx ->
                    "cast(number % ${1000 + idx} as int) as ${col}"
                }.join(",\n                        ")
                sql """
                    create table ${tableName} (
                        id int,
                        dt date,
                        ${dataColumns}
                    ) engine=iceberg
                    partition by list (day(dt)) ()
                    properties (
                        "format-version" = "3",
                        "write.format.default" = "${format}",
                        "write.delete.mode" = "merge-on-read",
                        "write.update.mode" = "merge-on-read",
                        "write.merge.mode" = "merge-on-read"
                    )
                """

                sql """
                    insert into ${tableName}
                    select
                        cast(number as int) as id,
                        cast(case when number % 2 = 0 then '2024-05-01' else '2024-05-02' end as date) as dt,
                        ${dataSelectExprs}
                    from numbers("number" = "${rowCount}")
                """

                sql """
                    update ${tableName}
                    set c001 = c001 + 1
                    where dt = date '2024-05-01' and id % 10 = 0
                """
                sql """
                    delete from ${tableName}
                    where dt = date '2024-05-02' and id % 20 = 1
                """

                long deletedRows = rowCount <= 1 ? 0L : (((rowCount - 2) / 20).toLong() + 1L)
                long expectedRows = rowCount - deletedRows
                def beforeRewriteChecksum = assertChecksum(tableName, expectedRows)
                assertPuffinDeleteFiles(tableName)

                def rewriteResult = sql("""
                    alter table ${catalogName}.${dbName}.${tableName}
                    execute rewrite_data_files(
                        "target-file-size-bytes" = "536870912",
                        "min-input-files" = "1"
                    )
                """)
                log.info("Large stability rewrite result for ${tableName}: ${rewriteResult}")
                assertTrue(rewriteResult.size() > 0,
                        "rewrite_data_files should return summary rows for ${tableName}")

                def afterRewriteChecksum = assertChecksum(tableName, expectedRows)
                assertEquals(beforeRewriteChecksum, afterRewriteChecksum)

                def lineageCounts = sql("""
                    select count(*), count(_row_id), count(_last_updated_sequence_number)
                    from ${tableName}
                """)
                log.info("Large stability lineage counts for ${tableName}: ${lineageCounts}")
                assertEquals(expectedRows, lineageCounts[0][0].toString().toLong())
                assertEquals(expectedRows, lineageCounts[0][1].toString().toLong())
                assertEquals(expectedRows, lineageCounts[0][2].toString().toLong())
            } finally {
                sql """drop table if exists ${tableName}"""
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
