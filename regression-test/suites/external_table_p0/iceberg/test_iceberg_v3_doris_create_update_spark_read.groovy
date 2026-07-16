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

suite("test_iceberg_v3_doris_create_update_spark_read", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_doris_create_update_spark_read"
    String dbName = "test_v3_doris_create_update_spark_read_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def formats = ["parquet", "orc"]
    def partitionFlags = [true, false]

    def checksum = { rows ->
        assertEquals(1, rows.size())
        return rows[0].collect { col -> col == null ? null : col.toString() }
    }

    def assertDorisSparkChecksumEqual = { tableName ->
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkChecksum = checksum(spark_iceberg("""
            select count(*), sum(id), sum(score),
                max(case when id = 2 then tag end)
            from demo.${dbName}.${tableName}
        """))
        def dorisChecksum = checksum(sql """
            select count(*), sum(id), sum(score),
                max(case when id = 2 then tag end)
            from ${tableName}
        """)
        log.info("Doris checksum for ${tableName}: ${dorisChecksum}")
        log.info("Spark checksum for ${tableName}: ${sparkChecksum}")
        assertEquals(dorisChecksum, sparkChecksum)
    }

    def assertDorisLineageReadable = { tableName ->
        def rows = sql """
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """
        log.info("Doris lineage rows for ${tableName}: ${rows}")
        assertEquals(3, rows.size())
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for ${tableName}, row=${row}")
            assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName}, row=${row}")
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
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""

    boolean sparkReady = false
    try {
        spark_iceberg """select 1"""
        sparkReady = true
        formats.each { format ->
            partitionFlags.each { partitioned ->
                String partitionSuffix = partitioned ? "part" : "unpart"
                String tableName = "doris_v3_update_spark_read_${format}_${partitionSuffix}"
                String partitionClause = partitioned ? "partition by list (day(dt)) ()" : ""

                sql """drop table if exists ${tableName}"""
                sql """
                    create table ${tableName} (
                        id int,
                        tag string,
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
                    (1, 'doris_v3_i', 100, date '2024-03-01'),
                    (2, 'doris_v3_i', 200, date '2024-03-02'),
                    (3, 'doris_v3_i', 300, date '2024-03-03')
                """
                sql """
                    update ${tableName}
                    set tag = 'doris_v3_u', score = score + 20
                    where id = 2
                """

                def rows = sql """
                    select id, tag, score
                    from ${tableName}
                    order by id
                """
                assertEquals(3, rows.size())
                assertEquals("doris_v3_u", rows[1][1].toString())
                assertEquals(220, rows[1][2].toString().toInteger())
                assertDorisLineageReadable(tableName)
                assertDorisSparkChecksumEqual(tableName)
            }
        }
    } catch (Exception e) {
        if (!sparkReady) {
            logger.info("spark-iceberg JDBC is unavailable, skip Doris v3 create/update Spark read test: ${e.message}")
            return
        }
        throw e
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
