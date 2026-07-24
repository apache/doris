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
        select id, cast(region as string) as region, tags, attrs, detail
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

    // WC01-S03: Iceberg allows Avro, but the current Doris writer supports
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
