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

// Sorted Iceberg writes run synchronously in the spill sink. Forced spilling makes every
// partition sorter write several spill runs, so the EOS sink call has to merge them before the
// data files are committed.
suite("test_iceberg_write_sorted_spill",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_sorted_spill"
    String dbName = "iceberg_write_sorted_spill_db"

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

    sql """drop table if exists sorted_spill"""
    sql """
        create table sorted_spill (
            id bigint,
            region string,
            payload string
        )
        order by (id desc nulls last)
        partition by list (region) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """

    // Each partition sorter receives several MiB, while forced spilling revokes the sink as soon
    // as it holds 1 MiB of sortable data.
    sql """set enable_spill = true"""
    sql """set enable_force_spill = true"""
    sql """set enable_reserve_memory = true"""
    sql """set spill_min_revocable_mem = 1"""
    sql """
        insert into sorted_spill
        select number,
               concat('R', number % 3),
               concat('payload-', number, '-', repeat('x', 200))
        from numbers('number' = '60000')
    """
    sql """set enable_force_spill = false"""
    sql """set enable_spill = false"""

    order_qt_sorted_spill_summary """
        select region, count(*), count(distinct id), min(id), max(id), sum(id),
               sum(length(payload))
        from sorted_spill
        group by region
    """
    order_qt_sorted_spill_rows """
        select id, region, payload
        from sorted_spill
        where id % 7919 = 0
    """

    // Every committed data file must keep the table order after the spill runs are merged.
    spark_iceberg """refresh table demo.${dbName}.sorted_spill"""
    def sparkSummary = spark_iceberg """
        select count(*), count(distinct _file), sum(id)
        from demo.${dbName}.sorted_spill
    """
    assertEquals(60000L, sparkSummary[0][0] as long)
    assertEquals(1799970000L, sparkSummary[0][2] as long)
    def unsortedRows = spark_iceberg """
        select count(*)
        from (
            select id, lag(id) over (partition by _file order by _pos) as previous_id
            from demo.${dbName}.sorted_spill
        ) ordered_rows
        where previous_id < id
    """
    assertEquals(0L, unsortedRows[0][0] as long)
}
