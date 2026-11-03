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

// Check physical Parquet split metadata for identity and transformed partitions.
suite("test_iceberg_write_partition_distribution",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_partition_distribution"
    String dbName = "iceberg_write_partition_distribution_db"

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
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""
    // Keep parallelism deterministic while staying below the per-writer partition cap.
    sql """set parallel_pipeline_task_num = 8"""

    // Identity partitions: multiple files must retain their own split offsets.
    sql """drop table if exists identity_partitions"""
    sql """
        create table identity_partitions (
            id int,
            p int
        )
        partition by list (p) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """
    sql """
        insert into identity_partitions
        select number, number % 20 from numbers("number" = "10000")
    """
    assertEquals([[10000L, 20L, 0, 9999]], sql("""
        select count(*), count(distinct p), min(id), max(id) from identity_partitions
    """))
    assertTrue(((sql "select count(*) from identity_partitions\$files")[0][0] as long) > 0L)
    assertEquals(0L, (sql """
        select count(*) from identity_partitions\$files
        where content = 0 and (split_offsets is null or size(split_offsets) = 0 or split_offsets[1] <= 0)
    """)[0][0] as long)

    // Transformed partitions also publish offsets when column metrics are disabled.
    sql """drop table if exists day_partitions"""
    sql """
        create table day_partitions (
            id int,
            ts datetime
        )
        partition by list (day(ts)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.metadata.metrics.default" = "none"
        )
    """
    sql """
        insert into day_partitions
        select number, hours_add(cast('2024-01-01 00:00:00' as datetime), number % 480)
        from numbers("number" = "10000")
    """
    assertEquals([[10000L, 20L]], sql("""
        select count(*), count(distinct date(ts)) from day_partitions
    """))
    assertTrue(((sql "select count(*) from day_partitions\$files")[0][0] as long) > 0L)
    // Disabling column metrics must not suppress the physical split boundaries in each data file.
    assertEquals(0L, (sql """
        select count(*) from day_partitions\$files
        where content = 0 and (split_offsets is null or size(split_offsets) = 0 or split_offsets[1] <= 0)
    """)[0][0] as long)

}
