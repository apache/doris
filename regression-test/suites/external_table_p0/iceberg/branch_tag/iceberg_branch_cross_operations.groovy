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

suite("iceberg_branch_cross_operations", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_cross"

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

    sql """drop database if exists ${catalog_name}.test_db_cross force"""
    sql """create database ${catalog_name}.test_db_cross"""
    sql """ use ${catalog_name}.test_db_cross """

    String table_name = "test_cross_branch"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """

    sql """ insert into ${table_name} values (1, 'a'), (2, 'b'), (3, 'c') """
    sql """ alter table ${table_name} create branch b1_cross """
    sql """ alter table ${table_name} create branch b2_cross """
    sql """ insert into ${table_name}@branch(b1_cross) values (4, 'd'), (5, 'e') """

    // Test 1.5.1: Read from branch and write to another branch
    qt_b1_initial """ select * from ${table_name}@branch(b1_cross) order by id """ // Should have 1,2,3,4,5
    qt_b2_initial """ select * from ${table_name}@branch(b2_cross) order by id """ // Should have 1,2,3

    sql """ insert into ${table_name}@branch(b2_cross) select * from ${table_name}@branch(b1_cross) """
    qt_b2_after_copy """ select * from ${table_name}@branch(b2_cross) order by id """ // Should have all data from b1
    qt_b1_unchanged """ select * from ${table_name}@branch(b1_cross) order by id """ // b1 should be unchanged

    // Test 1.5.2: Read from main branch and write to branch
    sql """ insert into ${table_name} values (6, 'f'), (7, 'g') """
    sql """ alter table ${table_name} create branch b3_cross """
    sql """ insert into ${table_name}@branch(b3_cross) select * from ${table_name} """
    qt_b3_has_main_data """ select * from ${table_name}@branch(b3_cross) order by id """ // Should have main branch data
    qt_main_unchanged_after_copy """ select * from ${table_name} order by id """ // Main should be unchanged

    // Test 1.5.3: Read from branch and overwrite main branch
    sql """ alter table ${table_name} create branch b4_cross """
    sql """ insert into ${table_name}@branch(b4_cross) values (8, 'h'), (9, 'i'), (10, 'j') """
    sql """ insert overwrite table ${table_name} select * from ${table_name}@branch(b4_cross) """
    qt_main_overwritten """ select * from ${table_name} order by id """ // Main should have b4 data
    qt_b4_unchanged """ select * from ${table_name}@branch(b4_cross) order by id """ // b4 should be unchanged

    // Test cross-branch operations with filters
    sql """ alter table ${table_name} create branch b5_cross """
    sql """ insert into ${table_name}@branch(b5_cross) values (11, 'k'), (12, 'l'), (13, 'm') """
    sql """ insert into ${table_name}@branch(b4_cross) select * from ${table_name}@branch(b5_cross) where id > 10 """
    qt_b4_has_filtered_data """ select * from ${table_name}@branch(b4_cross) order by id """ // Should have original + filtered data

    // Test overwrite from branch to branch
    sql """ alter table ${table_name} create branch b6_cross """
    sql """ insert into ${table_name}@branch(b6_cross) values (14, 'n'), (15, 'o') """
    sql """ insert overwrite table ${table_name}@branch(b6_cross) select * from ${table_name}@branch(b5_cross) """
    qt_b6_overwritten """ select * from ${table_name}@branch(b6_cross) order by id """ // Should have b5 data

    // Test multiple branch chain operations
    sql """ alter table ${table_name} create branch b7_cross """
    sql """ insert into ${table_name}@branch(b7_cross) values (16, 'p') """
    sql """ insert into ${table_name}@branch(b6_cross) select * from ${table_name}@branch(b7_cross) """
    sql """ insert into ${table_name}@branch(b5_cross) select * from ${table_name}@branch(b6_cross) """
    qt_b5_chain """ select count(*) from ${table_name}@branch(b5_cross) """
    qt_b6_chain """ select count(*) from ${table_name}@branch(b6_cross) """
    qt_b7_chain """ select count(*) from ${table_name}@branch(b7_cross) """
}

