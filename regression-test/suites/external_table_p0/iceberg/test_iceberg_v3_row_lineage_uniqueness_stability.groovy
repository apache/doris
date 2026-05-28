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

suite("test_iceberg_v3_row_lineage_uniqueness_stability",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_unique"
    String dbName = "test_row_lineage_unique_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

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
        String tableName = "test_row_lineage_unique_parquet"
        sql """drop table if exists ${tableName}"""
        sql """
            create table ${tableName} (
                id int,
                name string,
                score int,
                dt date
            ) engine=iceberg
            properties (
                "format-version" = "3",
                "write.format.default" = "parquet"
            )
        """

        sql """
            insert into ${tableName} values
                (1, 'a', 10, date '2024-08-01'),
                (2, 'b', 20, date '2024-08-02')
        """
        sql """
            insert into ${tableName} values
                (3, 'c', 30, date '2024-08-03'),
                (4, 'd', 40, date '2024-08-04')
        """
        sql """
            insert into ${tableName} values
                (5, 'e', 50, date '2024-08-05'),
                (6, 'f', 60, date '2024-08-06')
        """

        def rowLineageRows = sql """
            select id, _row_id, _last_updated_sequence_number
            from ${tableName}
            order by id
        """
        log.info("row lineage rows for ${tableName}: ${rowLineageRows}")
        assertEquals(6, rowLineageRows.size())
        rowLineageRows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null, row=${row}")
            assertTrue(row[2] != null, "_last_updated_sequence_number should be non-null, row=${row}")
        }

        def distinctRowIds = sql """
            select distinct _row_id
            from ${tableName}
            order by _row_id
        """
        log.info("distinct row lineage ids for ${tableName}: ${distinctRowIds}")
        assertEquals(6, distinctRowIds.size())

        def groupedRowIds = sql """
            select _row_id, count(*)
            from ${tableName}
            group by _row_id
            order by _row_id
        """
        log.info("grouped row lineage ids for ${tableName}: ${groupedRowIds}")
        assertEquals(6, groupedRowIds.size())
        groupedRowIds.each { row ->
            assertEquals(1L, row[1])
        }

        def aggregateRowIds = sql """
            select count(*), count(_row_id), count(distinct _row_id), ndv(_row_id)
            from ${tableName}
            where _row_id is not null
        """
        log.info("row lineage aggregate result for ${tableName}: ${aggregateRowIds}")
        assertEquals(6L, aggregateRowIds[0][0])
        assertEquals(6L, aggregateRowIds[0][1])
        assertEquals(6L, aggregateRowIds[0][2])
        assertEquals(6L, aggregateRowIds[0][3])
    } finally {
        sql """set show_hidden_columns = false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
