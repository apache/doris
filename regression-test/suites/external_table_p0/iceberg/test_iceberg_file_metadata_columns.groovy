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

suite("test_iceberg_file_metadata_columns", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_file_metadata_columns"
    String dbName = "test_iceberg_file_metadata_columns_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def verifyFileMetadata = { tableName, format ->
        sql """refresh table ${dbName}.${tableName}"""

        // Keep the golden result independent of UUID-based Iceberg data-file names.
        "order_qt_${format}_metadata_not_null" """
            select count(*), count(`_file`), count(`_pos`)
            from ${tableName}
        """

        def dorisRows = sql """
            select id, batch, `_file`, `_pos`
            from ${tableName}
            order by batch, id
        """
        assertEquals(6, dorisRows.size())

        // The Iceberg files metadata table is the authoritative source of data-file paths. This verifies
        // that `_file` is the original Iceberg data-file path rather than a rewritten scan path.
        def icebergFiles = spark_iceberg """
            select file_path, record_count
            from demo.${dbName}.${tableName}.files
            where content = 0
            order by file_path
        """
        assertEquals(2, icebergFiles.size(), "Expected one data file for each batch in ${tableName}")

        Set<String> expectedFiles = icebergFiles.collect { it[0].toString() }.toSet()
        Set<String> actualFiles = dorisRows.collect { it[2].toString() }.toSet()
        assertEquals(expectedFiles, actualFiles)

        Map<String, List<Long>> positionsByFile = [:]
        Map<Integer, Set<String>> filesByBatch = [:]
        dorisRows.each { row ->
            String file = row[2].toString()
            long pos = row[3].toString().toLong()
            int batch = row[1].toString().toInteger()
            positionsByFile.computeIfAbsent(file) { [] }.add(pos)
            filesByBatch.computeIfAbsent(batch) { [] }.add(file)
        }

        assertEquals(2, positionsByFile.size())
        assertEquals([1, 2] as Set, filesByBatch.keySet())
        filesByBatch.each { batch, files ->
            assertEquals(1, files.size(), "Batch ${batch} should be read from one data file")
        }
        positionsByFile.each { file, positions ->
            assertEquals([0L, 1L, 2L], positions.sort(),
                    "_pos must be the physical row position within ${file}")
        }

        // These deterministic aggregates verify the same per-file semantics in the generated .out file.
        "order_qt_${format}_metadata_per_file" """
            select batch, count(*), min(`_pos`), max(`_pos`), count(distinct `_file`)
            from ${tableName}
            group by batch, `_file`
            order by batch
        """
        "order_qt_${format}_metadata_file_count" """
            select count(distinct `_file`)
            from ${tableName}
        """
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
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
    sql """set enable_file_scanner_v2 = true"""
    spark_iceberg """create database if not exists demo.${dbName}"""

    test {
        sql """
            create table iceberg_reserved_file_metadata_column (
                `_file` string,
                id int
            ) engine=iceberg
        """
        exception "Cannot create Iceberg table with reserved metadata column: _file"
    }

    test {
        sql """
            create table iceberg_reserved_pos_metadata_column (
                `_pos` bigint,
                id int
            ) engine=iceberg
        """
        exception "Cannot create Iceberg table with reserved metadata column: _pos"
    }

    ["parquet", "orc"].each { format ->
        String tableName = "iceberg_file_metadata_${format}"
        spark_iceberg """drop table if exists demo.${dbName}.${tableName}"""
        spark_iceberg """
            create table demo.${dbName}.${tableName} (
                id int,
                batch int,
                payload string
            ) using iceberg
            partitioned by (batch)
            tblproperties (
                'format-version' = '2',
                'write.format.default' = '${format}',
                'write.distribution-mode' = 'none'
            )
        """
        spark_iceberg """
            insert into demo.${dbName}.${tableName} values
            (1, 1, 'one'),
            (2, 1, 'two'),
            (3, 1, 'three')
        """
        spark_iceberg """
            insert into demo.${dbName}.${tableName} values
            (4, 2, 'four'),
            (5, 2, 'five'),
            (6, 2, 'six')
        """

        verifyFileMetadata(tableName, format)
    }
}
