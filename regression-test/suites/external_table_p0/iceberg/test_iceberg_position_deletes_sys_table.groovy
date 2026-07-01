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

suite("test_iceberg_position_deletes_sys_table", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_position_deletes_sys_table"
    String dbName = "position_deletes_sys_table_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
    CREATE CATALOG ${catalogName} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${restPort}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
        "s3.region" = "us-east-1",
        "meta.cache.iceberg.table.ttl-second" = "0",
        "meta.cache.iceberg.schema.ttl-second" = "0"
    );"""

    spark_iceberg_multi """
        SET spark.sql.shuffle.partitions=1;
        CREATE DATABASE IF NOT EXISTS demo.${dbName};
        DROP TABLE IF EXISTS demo.${dbName}.pd_unpartitioned;
        CREATE TABLE demo.${dbName}.pd_unpartitioned (
            id INT,
            name STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_unpartitioned
        SELECT /*+ COALESCE(1) */ id, name FROM VALUES
            (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd') AS t(id, name);
        DELETE FROM demo.${dbName}.pd_unpartitioned WHERE id = 2;
        DROP TABLE IF EXISTS demo.${dbName}.pd_partitioned;
        CREATE TABLE demo.${dbName}.pd_partitioned (
            id INT,
            name STRING,
            dt STRING
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_partitioned
        SELECT /*+ COALESCE(1) */ id, name, dt FROM VALUES
            (1, 'a', '2026-06-26'),
            (2, 'b', '2026-06-26'),
            (3, 'c', '2026-06-27'),
            (4, 'd', '2026-06-27') AS t(id, name, dt);
        DELETE FROM demo.${dbName}.pd_partitioned WHERE id = 2;
        DROP TABLE IF EXISTS demo.${dbName}.pd_v3_unpartitioned;
        CREATE TABLE demo.${dbName}.pd_v3_unpartitioned (
            id INT,
            name STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_v3_unpartitioned
        SELECT /*+ COALESCE(1) */ id, name FROM VALUES
            (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name);
        DELETE FROM demo.${dbName}.pd_v3_unpartitioned WHERE id = 2;
        DROP TABLE IF EXISTS demo.${dbName}.pd_v3_partitioned;
        CREATE TABLE demo.${dbName}.pd_v3_partitioned (
            id INT,
            name STRING,
            dt STRING
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES (
            'format-version'='3',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_v3_partitioned
        SELECT /*+ COALESCE(1) */ id, name, dt FROM VALUES
            (1, 'a', '2026-07-01'),
            (2, 'b', '2026-07-01'),
            (3, 'c', '2026-07-02') AS t(id, name, dt);
        DELETE FROM demo.${dbName}.pd_v3_partitioned WHERE id = 2;
        DROP TABLE IF EXISTS demo.${dbName}.pd_schema_time_travel;
        CREATE TABLE demo.${dbName}.pd_schema_time_travel (
            id INT,
            name STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_schema_time_travel
        SELECT /*+ COALESCE(1) */ id, name FROM VALUES
            (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name);
        DELETE FROM demo.${dbName}.pd_schema_time_travel WHERE id = 2;
        ALTER TABLE demo.${dbName}.pd_schema_time_travel ADD COLUMN note STRING;
        INSERT INTO demo.${dbName}.pd_schema_time_travel
        SELECT /*+ COALESCE(1) */ id, name, note FROM VALUES
            (4, 'd', 'new'), (5, 'e', 'new') AS t(id, name, note);
        DELETE FROM demo.${dbName}.pd_schema_time_travel WHERE id = 4;
        DROP TABLE IF EXISTS demo.${dbName}.pd_no_deletes;
        CREATE TABLE demo.${dbName}.pd_no_deletes (
            id INT,
            name STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');
        INSERT INTO demo.${dbName}.pd_no_deletes VALUES (1, 'a'), (2, 'b');
    """, 300

    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    def assertPositionDeletesSchema = { String tableName, boolean partitioned, List<String> extraColumns = [] ->
        List<List<Object>> descRows = sql """desc ${tableName}\$position_deletes"""
        List<String> columns = descRows.collect { it[0].toString() }
        List<String> expectedColumns = ["file_path", "pos", "row"]
        if (partitioned) {
            expectedColumns.add("partition")
        }
        expectedColumns.addAll(["spec_id", "delete_file_path"])
        expectedColumns.addAll(extraColumns)
        assertEquals(expectedColumns, columns)
    }

    def countRows = { String query ->
        List<List<Object>> rows = sql query
        assertEquals(1, rows.size())
        return ((Number) rows[0][0]).longValue()
    }

    List<String> v3ExtraColumns = ["content_offset", "content_size_in_bytes"]

    assertPositionDeletesSchema("pd_unpartitioned", false)
    long unpartitionedCount = countRows("""select count(*) from pd_unpartitioned\$position_deletes""")
    assertTrue(unpartitionedCount > 0)
    assertEquals(unpartitionedCount, countRows("""select count(pos) from pd_unpartitioned\$position_deletes"""))
    assertEquals(unpartitionedCount, (long) sql(
            """select * from pd_unpartitioned\$position_deletes""").size())
    assertEquals(unpartitionedCount, (long) sql(
            """select pos from pd_unpartitioned\$position_deletes""").size())
    assertEquals(unpartitionedCount, (long) sql(
            """select delete_file_path, pos from pd_unpartitioned\$position_deletes""").size())
    assertEquals(unpartitionedCount, (long) sql(
            """select file_path, pos, delete_file_path from pd_unpartitioned\$position_deletes where pos >= 0""").size())
    assertEquals(unpartitionedCount, countRows(
            """select count(*) from pd_unpartitioned\$position_deletes where spec_id = 0"""))
    assertEquals(unpartitionedCount, countRows(
            """select count(*) from pd_unpartitioned\$position_deletes where delete_file_path is not null"""))
    assertEquals(0L, countRows("""select count(*) from pd_unpartitioned\$position_deletes where pos < 0"""))
    assertEquals(unpartitionedCount, countRows("""
            select count(*) from iceberg_meta(
                "table" = "${catalogName}.${dbName}.pd_unpartitioned",
                "query_type" = "position_deletes")
            """))
    assertEquals(
            sql("""select file_path, pos, delete_file_path from pd_unpartitioned\$position_deletes
                    order by file_path, pos"""),
            sql("""select file_path, pos, delete_file_path from iceberg_meta(
                    "table" = "${catalogName}.${dbName}.pd_unpartitioned",
                    "query_type" = "position_deletes")
                    order by file_path, pos"""))
    List<List<Object>> unpartitionedRows = sql """select `row` from pd_unpartitioned\$position_deletes"""
    assertEquals(unpartitionedCount, (long) unpartitionedRows.size())
    assertTrue(unpartitionedRows.every { it[0] == null })
    assertEquals([[1, "a"], [3, "c"], [4, "d"]], sql("""select * from pd_unpartitioned order by id"""))

    assertPositionDeletesSchema("pd_partitioned", true)
    long partitionedCount = countRows("""select count(*) from pd_partitioned\$position_deletes""")
    assertTrue(partitionedCount > 0)
    assertEquals(partitionedCount, countRows("""select count(pos) from pd_partitioned\$position_deletes"""))
    assertEquals(partitionedCount, (long) sql(
            """select * from pd_partitioned\$position_deletes""").size())
    assertEquals(partitionedCount, (long) sql(
            """select file_path, pos, delete_file_path from pd_partitioned\$position_deletes where pos >= 0""").size())
    assertEquals(partitionedCount, countRows(
            """select count(*) from pd_partitioned\$position_deletes where `partition` is not null"""))
    assertEquals(0L, countRows("""select count(*) from pd_partitioned\$position_deletes where pos < 0"""))
    List<List<Object>> partitionedRows = sql """select `row`, `partition` from pd_partitioned\$position_deletes"""
    assertEquals(partitionedCount, (long) partitionedRows.size())
    assertTrue(partitionedRows.every { it[0] == null && it[1] != null })
    assertEquals([[1, "a", "2026-06-26"], [3, "c", "2026-06-27"], [4, "d", "2026-06-27"]],
            sql("""select * from pd_partitioned order by id"""))

    assertPositionDeletesSchema("pd_v3_unpartitioned", false, v3ExtraColumns)
    long v3UnpartitionedCount = countRows("""select count(*) from pd_v3_unpartitioned\$position_deletes""")
    assertEquals(1L, v3UnpartitionedCount)
    assertEquals(v3UnpartitionedCount, countRows(
            """select count(pos) from pd_v3_unpartitioned\$position_deletes"""))
    assertEquals(v3UnpartitionedCount, countRows(
            """select count(content_offset) from pd_v3_unpartitioned\$position_deletes"""))
    assertEquals(v3UnpartitionedCount, countRows(
            """select count(content_size_in_bytes) from pd_v3_unpartitioned\$position_deletes"""))
    assertEquals(v3UnpartitionedCount, (long) sql(
            """select file_path, pos, delete_file_path, content_offset, content_size_in_bytes
                    from pd_v3_unpartitioned\$position_deletes""").size())
    assertEquals(v3UnpartitionedCount, countRows(
            """select count(*) from pd_v3_unpartitioned\$position_deletes
                    where content_offset >= 0 and content_size_in_bytes > 0"""))
    assertEquals(v3UnpartitionedCount, countRows(
            """select count(*) from pd_v3_unpartitioned\$position_deletes where spec_id = 0"""))
    assertEquals(v3UnpartitionedCount, countRows(
            """select count(*) from pd_v3_unpartitioned\$position_deletes where delete_file_path is not null"""))
    List<List<Object>> v3UnpartitionedRows = sql """select `row` from pd_v3_unpartitioned\$position_deletes"""
    assertEquals(v3UnpartitionedCount, (long) v3UnpartitionedRows.size())
    assertTrue(v3UnpartitionedRows.every { it[0] == null })
    assertEquals([[1, "a"], [3, "c"]], sql("""select * from pd_v3_unpartitioned order by id"""))

    assertPositionDeletesSchema("pd_v3_partitioned", true, v3ExtraColumns)
    long v3PartitionedCount = countRows("""select count(*) from pd_v3_partitioned\$position_deletes""")
    assertEquals(1L, v3PartitionedCount)
    assertEquals(v3PartitionedCount, countRows(
            """select count(pos) from pd_v3_partitioned\$position_deletes"""))
    assertEquals(v3PartitionedCount, countRows(
            """select count(content_offset) from pd_v3_partitioned\$position_deletes"""))
    assertEquals(v3PartitionedCount, countRows(
            """select count(content_size_in_bytes) from pd_v3_partitioned\$position_deletes"""))
    assertEquals(v3PartitionedCount, (long) sql(
            """select file_path, pos, delete_file_path, content_offset, content_size_in_bytes
                    from pd_v3_partitioned\$position_deletes""").size())
    assertEquals(v3PartitionedCount, countRows(
            """select count(*) from pd_v3_partitioned\$position_deletes
                    where content_offset >= 0 and content_size_in_bytes > 0"""))
    assertEquals(v3PartitionedCount, countRows(
            """select count(*) from pd_v3_partitioned\$position_deletes where `partition` is not null"""))
    assertEquals(v3PartitionedCount, countRows(
            """select count(*) from pd_v3_partitioned\$position_deletes where spec_id = 0"""))
    assertEquals(v3PartitionedCount, countRows(
            """select count(*) from pd_v3_partitioned\$position_deletes where delete_file_path is not null"""))
    List<List<Object>> v3PartitionedRows = sql """select `row`, `partition` from pd_v3_partitioned\$position_deletes"""
    assertEquals(v3PartitionedCount, (long) v3PartitionedRows.size())
    assertTrue(v3PartitionedRows.every { it[0] == null && it[1] != null })
    assertEquals([[1, "a", "2026-07-01"], [3, "c", "2026-07-02"]],
            sql("""select * from pd_v3_partitioned order by id"""))

    assertPositionDeletesSchema("pd_schema_time_travel", false)
    long schemaChangeCount = countRows("""select count(*) from pd_schema_time_travel\$position_deletes""")
    assertEquals(2L, schemaChangeCount)
    assertEquals(schemaChangeCount, countRows(
            """select count(*) from pd_schema_time_travel\$position_deletes where pos >= 0"""))
    assertEquals(schemaChangeCount, countRows(
            """select count(*) from pd_schema_time_travel\$position_deletes where spec_id = 0"""))
    assertEquals(schemaChangeCount, countRows(
            """select count(*) from pd_schema_time_travel\$position_deletes where delete_file_path is not null"""))
    assertEquals(0L, countRows("""select count(*) from pd_schema_time_travel\$position_deletes where pos < 0"""))
    assertEquals(
            sql("""select file_path, pos, delete_file_path from pd_schema_time_travel\$position_deletes
                    order by file_path, pos"""),
            sql("""select file_path, pos, delete_file_path from iceberg_meta(
                    "table" = "${catalogName}.${dbName}.pd_schema_time_travel",
                    "query_type" = "position_deletes")
                    order by file_path, pos"""))
    List<List<Object>> schemaChangeRows = sql """select `row` from pd_schema_time_travel\$position_deletes"""
    assertEquals(schemaChangeCount, (long) schemaChangeRows.size())
    assertTrue(schemaChangeRows.every { it[0] == null })
    assertEquals([[1, "a", null], [3, "c", null], [5, "e", "new"]],
            sql("""select * from pd_schema_time_travel order by id"""))

    List<List<Object>> schemaChangeSnapshots = sql """
            select snapshot_id from pd_schema_time_travel\$snapshots order by committed_at
            """
    assertEquals(4, schemaChangeSnapshots.size())
    Object afterInsertSnapshot = schemaChangeSnapshots.get(0)[0]
    Object afterFirstDeleteSnapshot = schemaChangeSnapshots.get(1)[0]
    Object afterSecondDeleteSnapshot = schemaChangeSnapshots.get(3)[0]
    assertEquals(0L, countRows("""
            select count(*) from pd_schema_time_travel\$position_deletes
            for version as of ${afterInsertSnapshot}
            """))
    assertEquals(1L, countRows("""
            select count(*) from pd_schema_time_travel\$position_deletes
            for version as of ${afterFirstDeleteSnapshot}
            """))
    assertEquals(2L, countRows("""
            select count(*) from pd_schema_time_travel\$position_deletes
            for version as of ${afterSecondDeleteSnapshot}
            """))
    assertEquals([[1, "a"], [2, "b"], [3, "c"]],
            sql("""select * from pd_schema_time_travel for version as of ${afterInsertSnapshot} order by id"""))
    assertEquals([[1, "a"], [3, "c"]],
            sql("""select * from pd_schema_time_travel for version as of ${afterFirstDeleteSnapshot} order by id"""))
    assertEquals([[1, "a", null], [3, "c", null], [5, "e", "new"]],
            sql("""select * from pd_schema_time_travel for version as of ${afterSecondDeleteSnapshot} order by id"""))

    assertPositionDeletesSchema("pd_no_deletes", false)
    assertEquals(0L, countRows("""select count(*) from pd_no_deletes\$position_deletes"""))
}
