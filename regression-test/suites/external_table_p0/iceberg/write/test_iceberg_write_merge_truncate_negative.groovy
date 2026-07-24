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

suite("test_iceberg_write_merge_truncate_negative",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    String crashTestEnabled = context.config.otherConfigs.get("enableIcebergCrashTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")
            || crashTestEnabled == null || !crashTestEnabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg crash test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_merge_truncate_negative"
    String dbName = "iceberg_write_merge_truncate_negative_db"

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

    sql """drop table if exists merge_truncate_negative"""
    sql """
        create table merge_truncate_negative (
            id int not null,
            partition_value string not null,
            payload string
        )
        partition by list (truncate(2, partition_value)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.merge.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read"
        )
    """
    sql """insert into merge_truncate_negative values (1, 'alpha', 'before')"""

    // WM03-S01: A MERGE source projection is nullable even when every source
    // value and the Iceberg target column are NOT NULL. The writer must reject
    // an invalid input as a query error and must never terminate a BE.
    sql """
        merge into merge_truncate_negative t
        using (
            select 1 as id, 'beta' as partition_value, 'after' as payload
            union all
            select 2, 'gamma', 'inserted'
        ) s
        on t.id = s.id
        when matched then update set
            partition_value = s.partition_value,
            payload = s.payload
        when not matched then
            insert (id, partition_value, payload)
            values (s.id, s.partition_value, s.payload)
    """
    order_qt_merge_truncate_after_fix """
        select id, partition_value, payload
        from merge_truncate_negative
        order by id
    """
}
