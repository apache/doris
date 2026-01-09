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

suite("iceberg_branch_tag_schema_change_extended", "p0,external,doris,external_docker,external_docker_doris,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_schema_extended"

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

    sql """drop database if exists ${catalog_name}.test_db_schema_ext force"""
    sql """create database ${catalog_name}.test_db_schema_ext"""
    sql """ use ${catalog_name}.test_db_schema_ext """

    String table_name = "test_schema_extended"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """
    sql """ insert into ${table_name} values (1, 'a'), (2, 'b') """

    // Test 3.1.1: Add column after branch query
    sql """ alter table ${table_name} create branch b1_schema """
    sql """ alter table ${table_name} add column new_col string """
    qt_b1_has_new_col """ select * from ${table_name}@branch(b1_schema) order by id """ // Should have new_col with NULL

    // Test 3.1.2: Drop column after branch query
    sql """ alter table ${table_name} create branch b2_schema """
    sql """ alter table ${table_name} drop column name """
    qt_b2_no_dropped_col """select * from ${table_name}@branch(b2_schema) order by id """ // Should not have 'name' column
//    qt_b2_no_dropped_col """ desc ${table_name}@branch(b2_schema) """ // Should not have 'name' column
//
    // Recreate table for next tests
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, value int) """
    sql """ insert into ${table_name} values (1, 10), (2, 20) """
    sql """ alter table ${table_name} create branch b3_schema """

    // Test 3.1.3: Modify column type after branch query
    sql """ alter table ${table_name} modify column id bigint """
    qt_b3_new_type """ desc ${table_name}@branch(b3_schema) """ // Should use new type

    // Test 3.1.4: Branch write with new schema
    sql """ alter table ${table_name} add column new_col string """
    sql """ insert into ${table_name}@branch(b3_schema)(id, value, new_col) values (3, 30, 'test') """
    qt_b3_with_new_col """ select * from ${table_name}@branch(b3_schema) where id = 3 """

    // Test 3.2.1: Add column after tag query
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """
    sql """ insert into ${table_name} values (1, 'a'), (2, 'b') """
    sql """ alter table ${table_name} create tag t1_schema """
    sql """ alter table ${table_name} add column new_col string """
    qt_t1_no_new_col """ select * from ${table_name}@tag(t1_schema) order by id """ // Should not have new_col

    // Test 3.2.2: Drop column after tag query
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, old_col string) """
    sql """ insert into ${table_name} values (1, 'a') """
    sql """ alter table ${table_name} create tag t2_schema """
    sql """ alter table ${table_name} drop column old_col """
    qt_t2_has_old_col """ select * from ${table_name}@tag(t2_schema) order by id """ // Should still have old_col

    // Test 3.2.3: Modify column type after tag query
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int) """
    sql """ insert into ${table_name} values (1), (2) """
    sql """ alter table ${table_name} create tag t3_schema """
    sql """ alter table ${table_name} modify column id bigint """
    qt_t3_old_type """ desc ${table_name}@tag(t3_schema) """ // Should still use INT

    // Test 3.2.4: Tag schema vs main schema comparison
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, col1 string) """
    sql """ insert into ${table_name} values (1, 'a') """
    sql """ alter table ${table_name} create tag t4_schema """
    sql """ alter table ${table_name} add column col2 string """
    sql """ alter table ${table_name} drop column col1 """
    sql """ alter table ${table_name} add column col3 int """

    qt_main_schema """ desc ${table_name} """ // Should have id, col2, col3
    qt_tag_schema """ desc ${table_name}@tag(t4_schema) """ // Should have id, col1

    // Test 3.3.1: Schema change in branch should fail
    // Note: Schema changes should be done on main table, not on branch/tag
    // This test verifies that attempting schema change on branch/tag is not supported
    test {
        sql """ alter table ${table_name}@branch(b1_schema) add column test_col string """
        exception
    }

    // Test 3.3.2: Schema change in tag should fail
    test {
        sql """ alter table ${table_name}@tag(t1_schema) add column test_col string """
        exception
    }

    // Test 3.3.3: Schema change with multiple branches
    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int) """
    sql """ insert into ${table_name} values (1), (2) """
    sql """ alter table ${table_name} create branch b4_schema """
    sql """ alter table ${table_name} create branch b5_schema """
    sql """ alter table ${table_name} create branch b6_schema """

    sql """ alter table ${table_name} add column new_col string """

    qt_b4_new_schema """ desc ${table_name}@branch(b4_schema) """ // Should have new_col
    qt_b5_new_schema """ desc ${table_name}@branch(b5_schema) """ // Should have new_col
    qt_b6_new_schema """ desc ${table_name}@branch(b6_schema) """ // Should have new_col
}

