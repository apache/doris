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
        DROP TABLE IF EXISTS demo.${dbName}.pd_int_partitioned;
        CREATE TABLE demo.${dbName}.pd_int_partitioned (
            id INT,
            name STRING,
            p INT
        ) USING iceberg
        PARTITIONED BY (p)
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_int_partitioned
        SELECT /*+ COALESCE(1) */ id, name, p FROM VALUES
            (1, 'a', 10),
            (2, 'b', 10),
            (3, 'c', 20) AS t(id, name, p);
        DELETE FROM demo.${dbName}.pd_int_partitioned WHERE id = 2;
        DROP TABLE IF EXISTS demo.${dbName}.pd_partition_evolution;
        CREATE TABLE demo.${dbName}.pd_partition_evolution (
            id INT,
            name STRING,
            p INT
        ) USING iceberg
        PARTITIONED BY (p)
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.distribution-mode'='none',
            'write.target-file-size-bytes'='134217728'
        );
        INSERT INTO demo.${dbName}.pd_partition_evolution
        SELECT /*+ COALESCE(1) */ id, name, p FROM VALUES
            (1, 'a', 10),
            (2, 'b', 10),
            (3, 'c', 20),
            (4, 'd', 20) AS t(id, name, p);
        DELETE FROM demo.${dbName}.pd_partition_evolution WHERE id = 2;
        ALTER TABLE demo.${dbName}.pd_partition_evolution ADD PARTITION FIELD bucket(1, id);
        INSERT INTO demo.${dbName}.pd_partition_evolution
        SELECT /*+ COALESCE(1) */ id, name, p FROM VALUES
            (5, 'e', 10),
            (6, 'f', 10) AS t(id, name, p);
        DELETE FROM demo.${dbName}.pd_partition_evolution WHERE id = 6;
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
    """

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

    def assertSparkDorisPositionDeletes = { String tableName, List<String> columns ->
        String columnList = columns.join(", ")
        String orderBy = "file_path, pos, delete_file_path"
        List<List<Object>> sparkRows = spark_iceberg """
                select ${columnList}
                from demo.${dbName}.${tableName}.position_deletes
                order by ${orderBy}
                """
        List<List<Object>> dorisRows = sql """
                select ${columnList}
                from ${tableName}\$position_deletes
                order by ${orderBy}
                """
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    List<String> v3ExtraColumns = ["content_offset", "content_size_in_bytes"]
    List<String> commonCompareColumns = ["file_path", "pos", "spec_id", "delete_file_path"]
    List<String> v3CompareColumns = commonCompareColumns + v3ExtraColumns

    assertPositionDeletesSchema("pd_unpartitioned", false)
    long unpartitionedCount = countRows("""select count(*) from pd_unpartitioned\$position_deletes""")
    assertTrue(unpartitionedCount > 0)
    assertSparkDorisPositionDeletes("pd_unpartitioned", commonCompareColumns)
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
    List<List<Object>> unpartitionedRows = sql """select `row` from pd_unpartitioned\$position_deletes"""
    assertEquals(unpartitionedCount, (long) unpartitionedRows.size())
    assertTrue(unpartitionedRows.every { it[0] == null })
    assertEquals([[1, "a"], [3, "c"], [4, "d"]], sql("""select * from pd_unpartitioned order by id"""))

    assertPositionDeletesSchema("pd_partitioned", true)
    long partitionedCount = countRows("""select count(*) from pd_partitioned\$position_deletes""")
    assertTrue(partitionedCount > 0)
    assertSparkDorisPositionDeletes("pd_partitioned", commonCompareColumns)
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

    assertPositionDeletesSchema("pd_int_partitioned", true)
    long intPartitionedCount = countRows("""select count(*) from pd_int_partitioned\$position_deletes""")
    assertTrue(intPartitionedCount > 0)
    assertEquals(intPartitionedCount, countRows("""select count(pos) from pd_int_partitioned\$position_deletes"""))
    assertEquals(intPartitionedCount, countRows(
            """select count(*) from pd_int_partitioned\$position_deletes where `partition` is not null"""))
    List<List<Object>> intPartitionedRows = sql """select `row`, `partition` from pd_int_partitioned\$position_deletes"""
    assertEquals(intPartitionedCount, (long) intPartitionedRows.size())
    assertTrue(intPartitionedRows.every { it[0] == null && it[1] != null })
    assertTrue(intPartitionedRows.every {
        String partitionValue = it[1].toString()
        partitionValue.contains("\"p\":10") && !partitionValue.contains("\"p\":\"10\"")
    })
    assertEquals([[1, "a", 10], [3, "c", 20]], sql("""select * from pd_int_partitioned order by id"""))

    assertPositionDeletesSchema("pd_partition_evolution", true)
    long partitionEvolutionCount = countRows("""select count(*) from pd_partition_evolution\$position_deletes""")
    assertEquals(2L, partitionEvolutionCount)
    assertSparkDorisPositionDeletes("pd_partition_evolution", commonCompareColumns)
    assertEquals(partitionEvolutionCount, countRows(
            """select count(pos) from pd_partition_evolution\$position_deletes"""))
    assertEquals(partitionEvolutionCount, countRows(
            """select count(*) from pd_partition_evolution\$position_deletes where `partition` is not null"""))
    List<List<Object>> partitionEvolutionRows = sql """
            select spec_id, cast(`partition` as string)
            from pd_partition_evolution\$position_deletes order by spec_id
            """
    assertEquals(2, partitionEvolutionRows.size())
    assertEquals(0, ((Number) partitionEvolutionRows[0][0]).intValue())
    assertEquals(1, ((Number) partitionEvolutionRows[1][0]).intValue())
    String oldSpecPartition = partitionEvolutionRows[0][1].toString()
    String newSpecPartition = partitionEvolutionRows[1][1].toString()
    assertTrue(oldSpecPartition.contains("\"p\":10"))
    assertTrue(oldSpecPartition.contains(":null"))
    assertTrue(newSpecPartition.contains("\"p\":10"))
    assertFalse(newSpecPartition.contains(":null"))
    assertEquals([[1, "a", 10], [3, "c", 20], [4, "d", 20], [5, "e", 10]],
            sql("""select * from pd_partition_evolution order by id"""))

    assertPositionDeletesSchema("pd_v3_unpartitioned", false, v3ExtraColumns)
    long v3UnpartitionedCount = countRows("""select count(*) from pd_v3_unpartitioned\$position_deletes""")
    assertEquals(1L, v3UnpartitionedCount)
    assertSparkDorisPositionDeletes("pd_v3_unpartitioned", v3CompareColumns)
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
    assertSparkDorisPositionDeletes("pd_v3_partitioned", v3CompareColumns)
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
    assertSparkDorisPositionDeletes("pd_no_deletes", commonCompareColumns)
    assertEquals(0L, countRows("""select count(*) from pd_no_deletes\$position_deletes"""))
}
