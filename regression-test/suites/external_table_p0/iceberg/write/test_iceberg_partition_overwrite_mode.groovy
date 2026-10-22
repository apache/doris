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

suite("test_iceberg_partition_overwrite_mode",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_partition_overwrite_mode"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """ switch ${catalog_name} """

    String db1 = catalog_name + "_db1"
    String tb1 = db1 + "_tb1"
    String tb_empty = db1 + "_tb_empty"

    sql """ drop database if exists ${db1} force"""
    sql """ create database ${db1} """
    sql """ create table ${db1}.${tb1} (id int, val int) partition by list (val)() """
    sql """ create table ${db1}.${tb_empty} (id int, val int) """

    sql """ use ${db1} """
    sql """ insert into ${tb1} values (1,1), (1,2), (1,3) """

    order_qt_before """ select * from ${tb1} order by val """

    // The default (no SET) is static, matching Spark's default partitionOverwriteMode=static: a bare
    // INSERT OVERWRITE clears the whole table first, so only the written row survives.
    sql """ insert overwrite table ${tb1} values (7,3) """

    order_qt_default_static_data """ select * from ${tb1} order by val """

    // ... and an empty source empties the table, even though it is partitioned.
    sql """ insert overwrite table ${tb1} select * from ${tb_empty} """

    order_qt_default_static_empty """ select * from ${tb1} order by val """

    // dynamic (Hive-compatible, Spark's partitionOverwriteMode=dynamic): only the written partition (val=2)
    // is replaced, the others survive.
    sql """ set iceberg_partition_overwrite_mode = 'dynamic' """
    sql """ insert into ${tb1} values (1,1), (1,2), (1,3) """
    sql """ insert overwrite table ${tb1} values (9,2) """

    order_qt_dynamic_data """ select * from ${tb1} order by val """

    // ... and an empty source is a no-op for a partitioned table.
    sql """ insert overwrite table ${tb1} select * from ${tb_empty} """

    order_qt_dynamic_empty """ select * from ${tb1} order by val """

    // A planner-proven empty source (WHERE FALSE) must still clear the table in static mode. With strict DML
    // consistency disabled (the cloud default) the empty relation reaches the sink directly, so the insert
    // must still run its transaction instead of taking the empty-insert shortcut.
    sql """ set enable_strict_consistency_dml = false """
    sql """ set iceberg_partition_overwrite_mode = 'static' """
    sql """ insert into ${tb1} values (1,1), (1,2), (1,3) """
    sql """ insert overwrite table ${tb1} select * from ${tb1} where false """

    order_qt_static_optimized_empty """ select * from ${tb1} order by val """

    sql """ set enable_strict_consistency_dml = true """
}
