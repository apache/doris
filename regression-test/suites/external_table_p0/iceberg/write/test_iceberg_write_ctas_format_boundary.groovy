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

suite("test_iceberg_write_ctas_format_boundary",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_ctas_format_boundary"
    String dbName = "iceberg_write_ctas_format_boundary_db"
    String internalDbName = "iceberg_write_ctas_format_boundary_internal_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0",
            "meta.cache.iceberg.schema.ttl-second" = "0"
        )
    """

    sql """drop database if exists internal.${internalDbName} force"""
    sql """create database internal.${internalDbName}"""
    sql """drop table if exists internal.${internalDbName}.ctas_source"""
    sql """
        create table internal.${internalDbName}.ctas_source (
            id int,
            region varchar(20),
            tags array<string>,
            attrs map<string, string>,
            detail struct<score:int,note:string>
        )
        duplicate key(id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDbName}.ctas_source values
            (1, 'A', ['x', null], map('k', 'v'), struct(10, 'one')),
            (2, null, [], map('null-value', null), struct(null, 'two')),
            (3, '中文', ['😀'], map(), struct(30, null))
    """

    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    // WC01-S01: CTAS preserves complex types, NULL values, partitioning and
    // writer properties when the source is a distributed Doris table.
    sql """drop table if exists ctas_complex_partitioned"""
    sql """
        create table ctas_complex_partitioned
        partition by list (region, bucket(4, id)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "orc",
            "write.orc.compression-codec" = "lz4"
        )
        as
        select id,
               cast(if(id in (1, 3), 'A', region) as string) as region,
               tags, attrs, detail
        from internal.${internalDbName}.ctas_source
    """
    order_qt_ctas_complex_rows """
        select id, region, tags, attrs, detail
        from ctas_complex_partitioned
        order by id
    """
    order_qt_ctas_complex_files """
        select lower(file_format), sum(record_count)
        from ctas_complex_partitioned\$files
        group by lower(file_format)
        order by lower(file_format)
    """
    order_qt_ctas_complex_partitions """
        select spec_id, count(*), sum(record_count)
        from ctas_complex_partitioned\$partitions
        group by spec_id
        order by spec_id
    """
    order_qt_ctas_complex_physical_partitions """
        select struct_element(`partition`, 'region'),
               struct_element(`partition`, 'id_bucket'),
               record_count
        from ctas_complex_partitioned\$partitions
        order by 1, 2
    """
    // Two rows share identity region A, so more than one physical partition
    // proves bucket(4,id) contributes to CTAS routing.
    assertTrue(((sql """
        select count(distinct struct_element(`partition`, 'id_bucket'))
        from ctas_complex_partitioned\$partitions
        where struct_element(`partition`, 'region') = 'A'
    """)[0][0] as long) > 1L)
    spark_iceberg """refresh table demo.${dbName}.ctas_complex_partitioned"""
    def sparkRows = spark_iceberg """
        select id, region, tags, attrs, detail
        from demo.${dbName}.ctas_complex_partitioned
        order by id
    """
    def dorisRows = sql """
        select id, region, tags, attrs, detail
        from ctas_complex_partitioned
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)
    def compressionProperty = spark_iceberg """
        show tblproperties demo.${dbName}.ctas_complex_partitioned
        ('write.orc.compression-codec')
    """
    // File-format checks do not prove the requested codec survived CTAS.
    assertEquals("lz4", compressionProperty[0][1].toString().toLowerCase())

    // WC01-S02: CTAS is atomic. A source expression failure must not leave a
    // visible Iceberg table or a partially committed snapshot.
    sql """set enable_strict_cast = true"""
    sql """drop table if exists ctas_failed_atomicity"""
    test {
        sql """
            create table ctas_failed_atomicity
            properties ("format-version" = "2")
            as
            select cast(if(number = 2, 'invalid-id', cast(number as string)) as int) as id,
                   concat('candidate-', number) as payload
            from numbers('number' = '8')
        """
        exception "can't cast to INT in strict mode"
    }
    assertEquals(0, (sql """show tables like 'ctas_failed_atomicity'""").size())

    // WC01-S03: FILE TVF must keep the Parquet schema spelling until Iceberg CTAS persists it.
    // The normalized names remain available for Doris runtime lookup.
    spark_iceberg_multi """
        DROP TABLE IF EXISTS demo.${dbName}.file_tvf_case_source;
        CREATE TABLE demo.${dbName}.file_tvf_case_source (
            id INT,
            payload STRUCT<CaseSensitive:BIGINT,
                           NestedArray:ARRAY<STRUCT<ArrayChild:BIGINT>>,
                           NestedMap:MAP<STRING,STRUCT<MapChild:BIGINT>>>
        ) USING iceberg
        TBLPROPERTIES ('write.format.default' = 'parquet');
        INSERT INTO demo.${dbName}.file_tvf_case_source VALUES (
            1,
            NAMED_STRUCT(
                'CaseSensitive', CAST(7 AS BIGINT),
                'NestedArray', ARRAY(NAMED_STRUCT('ArrayChild', CAST(8 AS BIGINT))),
                'NestedMap', MAP('k', NAMED_STRUCT('MapChild', CAST(9 AS BIGINT)))
            )
        );
    """
    sql """refresh catalog ${catalogName}"""
    String sourceFile = (sql """
        select file_path from file_tvf_case_source\$files order by file_path limit 1
    """)[0][0].toString()

    sql """drop table if exists ctas_file_tvf_case"""
    sql """
        create table ctas_file_tvf_case as
        select payload from file (
            "uri" = "${sourceFile}",
            "format" = "parquet",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "use_path_style" = "true"
        )
    """

    def ctasSchema = spark_iceberg """describe demo.${dbName}.ctas_file_tvf_case"""
    def payloadRow = ctasSchema.find { row -> row[0].toString() == "payload" }
    assertNotNull(payloadRow, "payload column should exist in the Iceberg CTAS schema")
    String payloadType = payloadRow[1].toString()
    assertTrue(payloadType.contains("CaseSensitive"), payloadType)
    assertTrue(payloadType.contains("NestedArray"), payloadType)
    assertTrue(payloadType.contains("ArrayChild"), payloadType)
    assertTrue(payloadType.contains("NestedMap"), payloadType)
    assertTrue(payloadType.contains("MapChild"), payloadType)

    def nestedValues = sql """
        select element_at(payload, 'casesensitive'),
               element_at(element_at(payload, 'nestedarray')[1], 'arraychild'),
               element_at(element_at(payload, 'nestedmap')['k'], 'mapchild')
        from ctas_file_tvf_case
    """
    assertEquals([[7L, 8L, 9L]], nestedValues)

    // WC01-S04: Names displayed from external metadata must remain executable even when Java ROOT lowercasing
    // changes their Unicode bytes or UTF-8 length before thrift reaches the BE.
    spark_iceberg_multi """
        DROP TABLE IF EXISTS demo.${dbName}.unicode_struct_fields;
        CREATE TABLE demo.${dbName}.unicode_struct_fields (
            id INT,
            payload STRUCT<`Σ`:BIGINT, `ẞ`:BIGINT>
        ) USING iceberg;
        INSERT INTO demo.${dbName}.unicode_struct_fields VALUES (
            1, NAMED_STRUCT('Σ', CAST(10 AS BIGINT), 'ẞ', CAST(11 AS BIGINT))
        );
    """
    sql """refresh catalog ${catalogName}"""
    def unicodeSchema = sql """describe unicode_struct_fields"""
    def unicodePayloadRow = unicodeSchema.find { row -> row[0].toString() == "payload" }
    assertNotNull(unicodePayloadRow, "payload column should exist in the Unicode Iceberg schema")
    String unicodePayloadType = unicodePayloadRow[1].toString()
    assertTrue(unicodePayloadType.contains("Σ"), unicodePayloadType)
    assertTrue(unicodePayloadType.contains("ẞ"), unicodePayloadType)
    def unicodeValues = sql """
        select element_at(payload, 'Σ'), element_at(payload, 'ẞ')
        from unicode_struct_fields
    """
    assertEquals([[10L, 11L]], unicodeValues)

    // WC01-S05: Iceberg allows Avro, but the current Doris writer supports
    // Parquet and ORC only. Reject Avro explicitly instead of silently falling back.
    sql """drop table if exists avro_write_boundary"""
    sql """
        create table avro_write_boundary (
            id int,
            payload string
        )
        properties (
            "format-version" = "2",
            "write.format.default" = "avro"
        )
    """
    long avroSnapshots = (sql """select count(*) from avro_write_boundary\$snapshots""")[0][0] as long
    test {
        sql """insert into avro_write_boundary values (1, 'must-not-fallback')"""
        exception "Unsupported input format type: avro"
    }
    assertEquals(avroSnapshots,
            (sql """select count(*) from avro_write_boundary\$snapshots""")[0][0] as long)
    assertEquals(0, (sql """select count(*) from avro_write_boundary\$files""")[0][0] as long)
}
