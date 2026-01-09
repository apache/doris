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

suite("iceberg_branch_partition_operations", "p0,external,doris,external_docker,external_docker_doris,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_partition"

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

    sql """drop database if exists ${catalog_name}.test_db_partition force"""
    sql """create database ${catalog_name}.test_db_partition"""
    sql """ use ${catalog_name}.test_db_partition """

    String table_name = "test_partition_branch"

    // Test 1.3.1: Partition table branch creation
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, par string) PARTITION BY LIST(par)() """

    sql """ insert into ${table_name} values (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b') """
    sql """ insert into ${table_name} values (5, 'c'), (6, 'c') """

    sql """ alter table ${table_name} create branch b1_partition """
    qt_b1_all_partitions """ select * from ${table_name}@branch(b1_partition) order by id """ // Should have all partitions

    // Test 1.3.2: Partition table branch write
    sql """ insert into ${table_name}@branch(b1_partition) values (7, 'd'), (8, 'd') """
    qt_b1_new_partition """ select * from ${table_name}@branch(b1_partition) where par = 'd' order by id """ // Should have new partition
    qt_main_no_new_partition """ select * from ${table_name} where par = 'd' order by id """ // Main branch should not have new partition

    // Test 1.3.3: Partition table branch overwrite
    sql """ insert into ${table_name} values (9, 'a'), (10, 'a') """
    qt_main_before_overwrite """ select * from ${table_name} where par = 'a' order by id """

    sql """ insert overwrite table ${table_name}@branch(b1_partition) partition(par='a') values (11, 'a'), (12, 'a') """
    qt_b1_overwrite_partition """ select * from ${table_name}@branch(b1_partition) where par = 'a' order by id """ // Should only have 11, 12
    qt_main_unchanged """ select * from ${table_name} where par = 'a' order by id """ // Main branch unchanged

    // Test multiple partitions in branch
    sql """ insert into ${table_name}@branch(b1_partition) values (13, 'e'), (14, 'e') """
    sql """ insert into ${table_name}@branch(b1_partition) values (15, 'f'), (16, 'f') """

    qt_b1_multiple_partitions """ select par, count(*) from ${table_name}@branch(b1_partition) group by par order by par """
    qt_main_partitions """ select par, count(*) from ${table_name} group by par order by par """

    // Test branch with different partition values
    sql """ alter table ${table_name} create branch b2_partition """
    sql """ insert into ${table_name}@branch(b2_partition) values (17, 'g'), (18, 'g') """
    sql """ insert into ${table_name}@branch(b2_partition) values (19, 'h'), (20, 'h') """

    qt_b2_partitions """ select * from ${table_name}@branch(b2_partition) order by id """
    qt_b1_still_has_original """ select count(*) from ${table_name}@branch(b1_partition) """ // b1 should still have its data
}

