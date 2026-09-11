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

suite("test_iceberg_v3_row_lineage_complex_query", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_complex_query"
    String dbName = "test_row_lineage_complex_query_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def partitionFlags = [false, true]

    def normalizeRows = { rows ->
        return rows.collect { row -> row.collect { col -> col == null ? null : col.toString() } }
    }

    def assertSparkDorisRows = { tableName, columns, orderBy, expectedRows = null ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkRows = spark_iceberg("""
            select ${columns}
            from demo.${dbName}.${tableName}
            order by ${orderBy}
        """)
        def dorisRows = sql("""
            select ${columns}
            from ${tableName}
            order by ${orderBy}
        """)
        log.info("Spark rows for ${tableName}: ${sparkRows}")
        log.info("Doris rows for ${tableName}: ${dorisRows}")
        assertSparkDorisResultEquals(sparkRows, dorisRows)

        def normalizedRows = normalizeRows(dorisRows)
        if (expectedRows != null) {
            assertEquals(expectedRows, normalizedRows)
        }
        return normalizedRows
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
                String tableName = "complex_${format}_${partitionSuffix}"
                String dimTable = "${tableName}_lineage_dim"
                String partitionClause = partitioned ? "partition by list (day(dt)) ()" : ""
                try {
                    sql """drop table if exists ${dimTable}"""
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
                            "write.format.default" = "${format}"
                        )
                    """

                    sql """
                        insert into ${tableName} values
                        (1, 'Alice', 10, date '2024-01-01'),
                        (2, 'Bob', 20, date '2024-01-01'),
                        (3, 'Carol', 30, date '2024-01-02'),
                        (4, 'Doris', 40, date '2024-01-02'),
                        (5, 'Eve', 50, date '2024-01-03')
                    """
                    assertSparkDorisRows(tableName, "id, name, score, dt", "id", [
                            ["1", "Alice", "10", "2024-01-01"],
                            ["2", "Bob", "20", "2024-01-01"],
                            ["3", "Carol", "30", "2024-01-02"],
                            ["4", "Doris", "40", "2024-01-02"],
                            ["5", "Eve", "50", "2024-01-03"]
                    ])

                    sql """
                        create table ${dimTable} (
                            id int,
                            rid bigint,
                            seq bigint,
                            tag string
                        ) engine=iceberg
                        properties (
                            "format-version" = "3",
                            "write.format.default" = "${format}"
                        )
                    """

                    sql """
                        insert into ${dimTable}(id, rid, seq, tag)
                        select id, _row_id, _last_updated_sequence_number, concat('tag_', cast(id as string))
                        from ${tableName}
                        where id in (1, 4, 5)
                    """

                    def dimRows = assertSparkDorisRows(dimTable, "id, rid, seq, tag", "id")
                    log.info("Checking INSERT SELECT into ordinary lineage columns for ${dimTable}: ${dimRows}")
                    assertEquals(3, dimRows.size())
                    dimRows.each { row ->
                        assertTrue(row[1] != null, "rid should be populated from _row_id for ${dimTable}, row=${row}")
                        assertTrue(row[2] != null,
                                "seq should be populated from _last_updated_sequence_number for ${dimTable}, row=${row}")
                    }

                    def joinRows = sql("""
                        select t.id, t.name, d.tag
                        from ${tableName} t
                        join ${dimTable} d
                          on t._row_id = d.rid
                        order by t.id
                    """)
                    assertEquals([
                            ["1", "Alice", "tag_1"],
                            ["4", "Doris", "tag_4"],
                            ["5", "Eve", "tag_5"]
                    ], normalizeRows(joinRows))

                    def aggregateRows = sql("""
                        select dt, count(*), min(_row_id), max(_last_updated_sequence_number)
                        from ${tableName}
                        group by dt
                        order by dt
                    """)
                    log.info("Checking row lineage aggregate for ${tableName}: ${aggregateRows}")
                    assertEquals(3, aggregateRows.size())
                    aggregateRows.each { row ->
                        assertTrue(row[2] != null, "min(_row_id) should be non-null for ${tableName}, row=${row}")
                        assertTrue(row[3] != null,
                                "max(_last_updated_sequence_number) should be non-null for ${tableName}, row=${row}")
                    }

                    def subqueryRows = sql("""
                        select id, name
                        from ${tableName}
                        where _row_id in (
                            select rid from ${dimTable} where tag like 'tag_%'
                        )
                        order by id
                    """)
                    assertEquals([
                            ["1", "Alice"],
                            ["4", "Doris"],
                            ["5", "Eve"]
                    ], normalizeRows(subqueryRows))

                    test {
                        sql """
                            insert into ${tableName}(_row_id, id, name, score, dt)
                            select rid, id, tag, 0, date '2024-01-01'
                            from ${dimTable}
                        """
                        exception "Cannot specify row lineage column '_row_id' in INSERT statement"
                    }

                    test {
                        sql """
                            insert into ${tableName}(_last_updated_sequence_number, id, name, score, dt)
                            select seq, id, tag, 0, date '2024-01-01'
                            from ${dimTable}
                        """
                        exception "Cannot specify row lineage column '_last_updated_sequence_number' in INSERT statement"
                    }
                } finally {
                    sql """drop table if exists ${dimTable}"""
                    sql """drop table if exists ${tableName}"""
                }
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
