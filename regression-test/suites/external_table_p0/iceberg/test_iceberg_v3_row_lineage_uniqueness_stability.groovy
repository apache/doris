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

suite("test_iceberg_v3_row_lineage_uniqueness_stability", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_uniqueness_stability"
    String dbName = "test_row_lineage_uniqueness_stability_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def partitionFlags = [false, true]

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

    def assertUniqueRowIds = { tableName ->
        def counts = sql("""
            select count(*), count(distinct _row_id)
            from ${tableName}
            where _row_id is not null
        """)
        log.info("Checking row_id uniqueness for ${tableName}: ${counts}")
        assertEquals(counts[0][0].toString().toLong(), counts[0][1].toString().toLong())
    }

    def assertSparkDorisBusinessRows = { tableName, expectedRows ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
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

        def normalizedRows = dorisRows.collect {
            [it[0].toString().toInteger(), it[1].toString(), it[2].toString().toInteger(), it[3].toString()]
        }
        assertEquals(expectedRows, normalizedRows)
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
            partitionFlags.each { partitioned ->
                String partitionSuffix = partitioned ? "part" : "unpart"
                String tableName = "unique_stable_${format}_${partitionSuffix}"
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
                    sql """insert into ${tableName} values (1, 'a', 10, date '2024-08-01'), (2, 'b', 20, date '2024-08-02')"""
                    sql """insert into ${tableName} values (3, 'c', 30, date '2024-08-03'), (4, 'd', 40, date '2024-08-04')"""
                    sql """insert into ${tableName} values (5, 'e', 50, date '2024-08-05'), (6, 'f', 60, date '2024-08-06')"""

                    Map<Integer, List<Long>> initialLineage = lineageMap(tableName)
                    assertUniqueRowIds(tableName)

                    sql """update ${tableName} set score = score + 10 where id = 1"""
                    Map<Integer, List<Long>> afterUpdateLineage = lineageMap(tableName)
                    assertUniqueRowIds(tableName)
                    assertEquals(initialLineage[1][0], afterUpdateLineage[1][0])
                    assertTrue(afterUpdateLineage[1][1] > initialLineage[1][1],
                            "UPDATE should advance sequence for id=1 in ${tableName}")

                    sql """
                        merge into ${tableName} t
                        using (
                            select 4 as id, 'stable_m' as name, 404 as score, date '2024-08-04' as dt
                        ) s
                        on t.id = s.id
                        when matched then update set name = s.name, score = s.score
                    """
                    Map<Integer, List<Long>> afterMergeLineage = lineageMap(tableName)
                    assertUniqueRowIds(tableName)
                    assertEquals(initialLineage[4][0], afterMergeLineage[4][0])
                    assertTrue(afterMergeLineage[4][1] > initialLineage[4][1],
                            "MERGE UPDATE should advance sequence for id=4 in ${tableName}")

                    def rewriteResult = sql("""
                        alter table ${catalogName}.${dbName}.${tableName}
                        execute rewrite_data_files(
                            "target-file-size-bytes" = "10485760",
                            "min-input-files" = "1"
                        )
                    """)
                    log.info("rewrite_data_files result for ${tableName}: ${rewriteResult}")
                    assertTrue(rewriteResult.size() > 0,
                            "rewrite_data_files should return summary rows for ${tableName}")

                    Map<Integer, List<Long>> afterRewriteLineage = lineageMap(tableName)
                    assertUniqueRowIds(tableName)
                    assertEquals(afterUpdateLineage[1][0], afterRewriteLineage[1][0])
                    assertEquals(afterMergeLineage[4][0], afterRewriteLineage[4][0])
                    assertEquals(afterMergeLineage[4][1], afterRewriteLineage[4][1])

                    assertSparkDorisBusinessRows(tableName, [
                            [1, "a", 20, "2024-08-01"],
                            [2, "b", 20, "2024-08-02"],
                            [3, "c", 30, "2024-08-03"],
                            [4, "stable_m", 404, "2024-08-04"],
                            [5, "e", 50, "2024-08-05"],
                            [6, "f", 60, "2024-08-06"]
                    ])
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
