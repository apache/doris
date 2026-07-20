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

// Regression for a BE crash when reading the hidden Iceberg row-id column on a
// format-version 1 table:
//
//   F ... vparquet_group_reader.cpp:787 Check failed: block->rows() == col.column->size()
//         block rows = 1 , column rows = 0, col name = __DORIS_ICEBERG_ROWID_COL__
//
// The FE adds the hidden __DORIS_ICEBERG_ROWID_COL__ column whenever
// show_hidden_columns=true, regardless of the Iceberg format version
// (IcebergExternalTable.getFullSchema). The BE, however, only enabled row-id
// generation for format-version >= 2, because IcebergTableReader::init_row_filters
// set up the row-id params *after* the delete-files version gate
// (version < MIN_SUPPORT_DELETE_FILES_VERSION returns early). On a v1 table a
// SELECT * therefore left the row-id column empty while every other column was
// filtered/filled to the surviving row count, tripping the consistency check in
// RowGroupReader::_do_lazy_read.
//
// The fix moves the row-id setup ahead of the version gate, so this test must
// return correct results (and not crash the BE) for v1 tables.
suite("test_iceberg_v1_rowid_hidden_column", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v1_rowid_hidden_column"
    String dbName = "test_iceberg_v1_rowid_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def descColumns = { rows -> rows.collect { row -> row[0].toString().toLowerCase() } }

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

    // Run the full set of repro queries against one v1 table (3 visible columns: id, s, age).
    def runChecks = { tbl, format ->
        sql """
            insert into ${tbl} values
            (1, 'a', 25),
            (2, 'a', 30),
            (3, 'b', 35),
            (4, 'c', 40)
        """

        // 1. Default visibility: the row-id hidden column is not exposed.
        sql """set show_hidden_columns = false"""
        def visible = sql """select * from ${tbl} order by id"""
        assertEquals(4, visible.size())
        assertEquals(3, visible[0].size())

        // 2. show_hidden_columns=true exposes only __DORIS_ICEBERG_ROWID_COL__ on a v1 table
        //    (v3 row lineage columns must NOT appear).
        sql """set show_hidden_columns = true"""
        def hiddenCols = descColumns(sql("""desc ${tbl}"""))
        log.info("v1 desc with show_hidden_columns=true (${format}, ${tbl}) -> ${hiddenCols}")
        assertTrue(hiddenCols.any { it.contains("doris_iceberg_rowid") },
                "show_hidden_columns should expose __DORIS_ICEBERG_ROWID_COL__ for v1 table, got ${hiddenCols}")
        assertTrue(!hiddenCols.contains("_row_id"),
                "v1 table must not expose v3 row lineage column _row_id, got ${hiddenCols}")
        assertTrue(!hiddenCols.contains("_last_updated_sequence_number"),
                "v1 table must not expose v3 row lineage column _last_updated_sequence_number, got ${hiddenCols}")

        // 3. Crash repro: SELECT * with a predicate on the string column that filters rows.
        //    Before the fix the BE aborted here (row-id column left at 0 rows).
        def byString = sql """select * from ${tbl} where s = 'a' order by id"""
        log.info("v1 SELECT * where s='a' (${format}, ${tbl}) -> ${byString}")
        assertEquals(2, byString.size())
        assertEquals(4, byString[0].size()) // id, s, age, __DORIS_ICEBERG_ROWID_COL__
        assertEquals(1, byString[0][0].toString().toInteger())
        assertEquals(2, byString[1][0].toString().toInteger())
        assertTrue(byString[0][byString[0].size() - 1] != null,
                "row-id column must be populated, got null: ${byString[0]}")

        // 4. Crash repro: predicate on a data column (exercises lazy materialization, the path
        //    in the reported stack) with other lazy columns in the projection.
        def byData = sql """select * from ${tbl} where id > 1 order by id"""
        log.info("v1 SELECT * where id>1 (${format}, ${tbl}) -> ${byData}")
        assertEquals(3, byData.size())
        assertEquals(4, byData[0].size())
        assertEquals([2, 3, 4], byData.collect { it[0].toString().toInteger() })

        // 5. Full scan with the hidden column also returns every row.
        def all = sql """select * from ${tbl} order by id"""
        assertEquals(4, all.size())
        assertEquals(4, all[0].size())

        sql """set show_hidden_columns = false"""
    }

    def formats = ["parquet", "orc"]
    try {
        formats.each { format ->
            String unpartitioned = "test_iceberg_v1_rowid_${format}"
            String partitioned = "test_iceberg_v1_rowid_part_${format}"
            log.info("Run v1 row-id hidden-column test with format ${format}")
            try {
                // Unpartitioned v1 table: row-id with partition_spec_id = 0 / empty partition data.
                sql """drop table if exists ${unpartitioned}"""
                sql """
                    create table ${unpartitioned} (
                        id int,
                        s string,
                        age int
                    ) engine=iceberg
                    properties (
                        "format-version" = "1",
                        "write.format.default" = "${format}"
                    )
                """
                runChecks(unpartitioned, format)

                // Partitioned v1 table mirroring the original crash (a Spark-written partitioned
                // v1 table), so the row-id struct's partition_spec_id / partition_data are exercised.
                sql """drop table if exists ${partitioned}"""
                sql """
                    create table ${partitioned} (
                        id int,
                        s string,
                        age int
                    ) engine=iceberg
                    partition by list (s) ()
                    properties (
                        "format-version" = "1",
                        "write.format.default" = "${format}"
                    )
                """
                runChecks(partitioned, format)
            } finally {
                sql """set show_hidden_columns = false"""
                sql """drop table if exists ${partitioned}"""
                sql """drop table if exists ${unpartitioned}"""
            }
        }
    } finally {
        sql """set show_hidden_columns = false"""
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
