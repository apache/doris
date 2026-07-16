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

suite("test_iceberg_v3_row_lineage_equality_delete_dv_interop", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_equality_delete_dv_interop"
    String dbName = "test_row_lineage_eq_delete_dv_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"
    String runSuffix = "${System.currentTimeMillis()}"

    def fixtureMetadataFiles = [
            parquet: "s3a://warehouse/wh/multi_catalog/equality_delete_par_1/metadata/00011-f7268cac-dd2f-4061-a2c9-b5aa54ab52f4.metadata.json",
            orc: "s3a://warehouse/wh/multi_catalog/equality_delete_orc_1/metadata/00011-568385a2-14af-4237-b186-ce46d350be19.metadata.json"
    ]

    def hasSparkIcebergJdbc = {
        try {
            spark_iceberg_jdbc """select 1"""
            return true
        } catch (Exception e) {
            logger.info("Check spark-iceberg JDBC failed: ${e.message}")
            return false
        }
    }

    def unregisterSparkTable = { tableName ->
        try {
            spark_iceberg_jdbc """
                call demo.system.unregister_table(table => '${dbName}.${tableName}')
            """
        } catch (Throwable t) {
            logger.info("Skip unregister ${dbName}.${tableName}: ${t.message}")
        }
    }

    def refreshTable = { tableName ->
        spark_iceberg_jdbc """refresh table demo.${dbName}.${tableName}"""
        sql """refresh table ${dbName}.${tableName}"""
    }

    def assertSparkDorisBusinessRows = { tableName ->
        refreshTable(tableName)
        def sparkRows = spark_iceberg_jdbc("""
            select new_new_id, new_name, data, id
            from demo.${dbName}.${tableName}
            order by new_new_id, new_name, data, id
        """)
        def dorisRows = sql("""
            select new_new_id, new_name, data, id
            from ${tableName}
            order by new_new_id, new_name, data, id
        """)
        log.info("Spark business rows for ${tableName}: ${sparkRows}")
        log.info("Doris business rows for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    def assertSparkDorisAggregateRows = { tableName ->
        refreshTable(tableName)
        def sparkRows = spark_iceberg_jdbc("""
            select count(*), count(distinct new_new_id)
            from demo.${dbName}.${tableName}
        """)
        def dorisRows = sql("""
            select count(*), count(distinct new_new_id)
            from ${tableName}
        """)
        log.info("Spark aggregate rows for ${tableName}: ${sparkRows}")
        log.info("Doris aggregate rows for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    def assertSparkDorisRowsByPredicate = { tableName, predicate ->
        refreshTable(tableName)
        def sparkRows = spark_iceberg_jdbc("""
            select new_new_id, new_name, data, id
            from demo.${dbName}.${tableName}
            where ${predicate}
            order by new_new_id, new_name, data, id
        """)
        def dorisRows = sql("""
            select new_new_id, new_name, data, id
            from ${tableName}
            where ${predicate}
            order by new_new_id, new_name, data, id
        """)
        log.info("Spark rows for ${tableName} with predicate ${predicate}: ${sparkRows}")
        log.info("Doris rows for ${tableName} with predicate ${predicate}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    def assertLineageProjectionReadable = { tableName ->
        def rows = sql("""
            select new_new_id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by new_new_id
        """)
        logger.info("Row lineage projection after equality delete and Puffin DV for ${tableName}: ${rows}")
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for ${tableName}, row=${row}")
            assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName}, row=${row}")
        }
    }

    def deleteFileFormats = { tableName ->
        refreshTable(tableName)
        def sparkRows = spark_iceberg_jdbc("""
            select lower(file_format), count(*)
            from demo.${dbName}.${tableName}.delete_files
            group by lower(file_format)
            order by lower(file_format)
        """)
        def dorisRows = sql("""
            select lower(file_format), count(*)
            from ${tableName}\$delete_files
            group by lower(file_format)
            order by lower(file_format)
        """)
        log.info("Spark delete file formats for ${tableName}: ${sparkRows}")
        log.info("Doris delete file formats for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)
        return dorisRows
    }

    def assertOnlyEqualityDeleteFilesBeforeDorisDelete = { tableName ->
        def deleteFiles = deleteFileFormats(tableName)
        logger.info("Delete files before Doris v3 DELETE for ${tableName}: ${deleteFiles}")
        assertTrue(deleteFiles.size() > 0, "Fixture should contain equality delete files for ${tableName}")
        assertTrue(!deleteFiles.any { row -> row[0].toString() == "puffin" },
                "Fixture should not contain Puffin DV before Doris DELETE for ${tableName}: ${deleteFiles}")
    }

    def assertEqualityDeleteAndPuffinDvCoexist = { tableName ->
        def deleteFiles = deleteFileFormats(tableName)
        logger.info("Delete files after Doris v3 DELETE for ${tableName}: ${deleteFiles}")
        assertTrue(deleteFiles.any { row -> row[0].toString() == "puffin" },
                "Doris v3 DELETE should write Puffin DV for ${tableName}: ${deleteFiles}")
        assertTrue(deleteFiles.any { row -> row[0].toString() != "puffin" },
                "Existing equality delete files should still be visible with Puffin DV for ${tableName}: ${deleteFiles}")
    }

    if (!hasSparkIcebergJdbc()) {
        logger.info("spark-iceberg JDBC is unavailable, skip equality delete + v3 Puffin DV branch")
        return
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

    String oldVersionPredicate = """
        (new_new_id = 1 and new_name in ('smith', 'smith2', 'smith3'))
        or (new_new_id = 2 and new_name = 'danny')
        or (new_new_id = 4 and new_name in ('bob', 'bob2'))
        or (new_new_id = 5 and new_name = 'dennis')
    """

    try {
        spark_iceberg_jdbc """create database if not exists demo.${dbName}"""

        fixtureMetadataFiles.each { format, metadataFile ->
            String tableName = "eq_delete_dv_${format}_${runSuffix}"
            try {
                spark_iceberg_jdbc_multi """
                    call demo.system.register_table(
                        table => '${dbName}.${tableName}',
                        metadata_file => '${metadataFile}'
                    );
                    alter table demo.${dbName}.${tableName}
                    set tblproperties (
                        'format-version' = '3',
                        'write.delete.mode' = 'merge-on-read',
                        'write.update.mode' = 'merge-on-read',
                        'write.merge.mode' = 'merge-on-read'
                    );
                """

                assertSparkDorisBusinessRows(tableName)
                assertSparkDorisAggregateRows(tableName)
                assertSparkDorisRowsByPredicate(tableName, oldVersionPredicate)
                assertLineageProjectionReadable(tableName)
                assertOnlyEqualityDeleteFilesBeforeDorisDelete(tableName)

                sql """
                    delete from ${tableName}
                    where new_new_id = 2
                """

                assertSparkDorisBusinessRows(tableName)
                assertSparkDorisAggregateRows(tableName)
                assertSparkDorisRowsByPredicate(tableName, "new_new_id = 2")
                assertSparkDorisRowsByPredicate(tableName, oldVersionPredicate)
                assertLineageProjectionReadable(tableName)
                assertEqualityDeleteAndPuffinDvCoexist(tableName)
            } finally {
                unregisterSparkTable(tableName)
            }
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
