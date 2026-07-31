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

suite("test_iceberg_write_source_models",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_source_models"
    String dbName = "iceberg_write_source_models_db"
    String internalDb = "iceberg_write_source_models_internal_db"

    sql """drop database if exists internal.${internalDb} force"""
    sql """create database internal.${internalDb}"""

    // W04-S01: Duplicate model, no source partition, RANDOM distribution with three buckets.
    sql """drop table if exists internal.${internalDb}.source_duplicate"""
    sql """
        create table internal.${internalDb}.source_duplicate (
            id int,
            category varchar(20),
            amount bigint
        )
        duplicate key(id)
        distributed by random buckets 3
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.source_duplicate values
            (1, 'A', 10),
            (1, 'A', 11),
            (2, null, 20)
    """

    // W04-S02: Unique MOW model, LIST source partition and HASH AUTO buckets.
    sql """drop table if exists internal.${internalDb}.source_unique_mow"""
    sql """
        create table internal.${internalDb}.source_unique_mow (
            id int,
            category varchar(20),
            amount bigint
        )
        unique key(id, category)
        partition by list(category) (
            partition p_ab values in ('A', 'B'),
            partition p_null values in (null)
        )
        distributed by hash(id) buckets auto
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """insert into internal.${internalDb}.source_unique_mow values (10, 'A', 100), (11, null, 110)"""
    sql """insert into internal.${internalDb}.source_unique_mow values (10, 'A', 101)"""

    // W04-S03: Unique MOR model, RANGE source partition and fixed HASH buckets.
    sql """drop table if exists internal.${internalDb}.source_unique_mor"""
    sql """
        create table internal.${internalDb}.source_unique_mor (
            id int,
            category varchar(20),
            amount bigint
        )
        unique key(id)
        partition by range(id) (
            partition p_lt_20 values less than (20),
            partition p_max values less than maxvalue
        )
        distributed by hash(id) buckets 2
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        )
    """
    sql """insert into internal.${internalDb}.source_unique_mor values (20, 'C', 200), (21, 'D', 210)"""
    sql """insert into internal.${internalDb}.source_unique_mor values (20, 'C', 201)"""

    // W04-S04: Aggregate model, RANGE source partition and four fixed HASH buckets.
    sql """drop table if exists internal.${internalDb}.source_aggregate"""
    sql """
        create table internal.${internalDb}.source_aggregate (
            id int,
            category varchar(20),
            amount bigint sum
        )
        aggregate key(id, category)
        partition by range(id) (
            partition p_lt_40 values less than (40),
            partition p_max values less than maxvalue
        )
        distributed by hash(id, category) buckets 4
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.source_aggregate values
            (30, 'E', 300),
            (30, 'E', 3),
            (31, 'F', 310)
    """

    order_qt_internal_model_oracle """
        select 'duplicate', id, category, amount
        from internal.${internalDb}.source_duplicate
        union all
        select 'unique_mow', id, category, amount
        from internal.${internalDb}.source_unique_mow
        union all
        select 'unique_mor', id, category, amount
        from internal.${internalDb}.source_unique_mor
        union all
        select 'aggregate', id, category, amount
        from internal.${internalDb}.source_aggregate
        order by 1, 2, 3, 4
    """

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
    sql """set enable_fallback_to_original_planner = false"""

    sql """drop table if exists source_model_sink"""
    sql """
        create table source_model_sink (
            source_model string not null,
            id int,
            category string,
            amount bigint
        )
        partition by list (source_model, bucket(4, category)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """

    // W04-S05: Independent INSERT SELECT statements keep each source model's read semantics.
    // Multiple source buckets exercise distributed sink writers on more than one BE.
    sql """
        insert into source_model_sink
        select 'duplicate', id, category, amount
        from internal.${internalDb}.source_duplicate
    """
    sql """
        insert into source_model_sink
        select 'unique_mow', id, category, amount
        from internal.${internalDb}.source_unique_mow
    """
    sql """
        insert into source_model_sink
        select 'unique_mor', id, category, amount
        from internal.${internalDb}.source_unique_mor
    """
    sql """
        insert into source_model_sink
        select 'aggregate', id, category, amount
        from internal.${internalDb}.source_aggregate
    """

    order_qt_source_model_sink """
        select source_model, id, category, amount
        from source_model_sink
        order by source_model, id, category, amount
    """
    order_qt_source_model_partition_stats """
        select spec_id, sum(record_count)
        from source_model_sink\$partitions
        group by spec_id
        order by spec_id
    """

    sql """refresh table ${dbName}.source_model_sink"""
    spark_iceberg """refresh table demo.${dbName}.source_model_sink"""
    def sparkRows = spark_iceberg """
        select source_model, id, category, amount
        from demo.${dbName}.source_model_sink
        order by source_model, id, category, amount
    """
    def dorisRows = sql """
        select source_model, id, category, amount
        from source_model_sink
        order by source_model, id, category, amount
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)
}
