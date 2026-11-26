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

// A partitioned INSERT is routed by the Iceberg partition value, so one partition reaches one writer. A write
// touching more than table_sink_partition_write_max_partition_nums_per_writer (128) partitions then succeeds,
// and each partition gets one data file instead of one per writer.
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
    // The writers follow the source fragment's parallelism, and the 128-partition cap applies to the partitions
    // routed to each writer. Pin enough writers for 300 partitions: a fuzzy session can pick one or two.
    sql """set parallel_pipeline_task_num = 8"""

    // Identity partitions: 200 partitions in one INSERT.
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
    qt_identity_plan """
        explain shape plan
        insert into identity_partitions
        select number, number % 200 from numbers("number" = "10000")
    """
    sql """
        insert into identity_partitions
        select number, number % 200 from numbers("number" = "10000")
    """
    qt_identity_rows """
        select count(*), count(distinct p), min(id), max(id) from identity_partitions
    """
    // One writer owns each partition, so every partition holds exactly one data file.
    qt_identity_files """
        select count(*), max(file_count)
        from (
            select `partition`, count(*) as file_count
            from identity_partitions\$files
            group by `partition`
        ) partition_file_counts
    """

    assertEquals(0L, (sql """
        select count(*) from identity_partitions\$files
        where content = 0 and (split_offsets is null or size(split_offsets) = 0 or split_offsets[1] <= 0)
    """)[0][0] as long)

    // Transform partitions: rows of one day carry different timestamps, so routing by the raw source value would
    // split a day across writers. The hash must cover day(ts).
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
    qt_day_plan """
        explain shape plan
        insert into day_partitions
        select number, hours_add(cast('2024-01-01 00:00:00' as datetime), number % 7200)
        from numbers("number" = "10000")
    """
    sql """
        insert into day_partitions
        select number, hours_add(cast('2024-01-01 00:00:00' as datetime), number % 7200)
        from numbers("number" = "10000")
    """
    qt_day_rows """
        select count(*), count(distinct date(ts)), min(ts), max(ts) from day_partitions
    """
    qt_day_files """
        select count(*), max(file_count)
        from (
            select `partition`, count(*) as file_count
            from day_partitions\$files
            group by `partition`
        ) partition_file_counts
    """
    // Disabling column metrics must not suppress the physical split boundaries in each data file.
    assertEquals(0L, (sql """
        select count(*) from day_partitions\$files
        where content = 0 and (split_offsets is null or size(split_offsets) = 0 or split_offsets[1] <= 0)
    """)[0][0] as long)

}
