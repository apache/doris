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

suite("iceberg_branch_tag_edge_cases", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_edge_cases"

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

    sql """drop database if exists ${catalog_name}.test_db_edge force"""
    sql """create database ${catalog_name}.test_db_edge"""
    sql """ use ${catalog_name}.test_db_edge """

    String table_name = "test_edge_cases"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """
    sql """ insert into ${table_name} values (1, 'a'), (2, 'b') """

    // Test 1.6.1: Special characters in branch/tag names
    sql """ alter table ${table_name} create branch "branch-1" """
    sql """ alter table ${table_name} create branch "branch_1" """
    sql """ alter table ${table_name} create tag "tag-1" """
    sql """ alter table ${table_name} create tag "tag_1" """

    qt_branch_hyphen """ select * from ${table_name}@branch("branch-1") order by id """
    qt_branch_underscore """ select * from ${table_name}@branch("branch_1") order by id """
    qt_tag_hyphen """ select * from ${table_name}@tag("tag-1") order by id """
    qt_tag_underscore """ select * from ${table_name}@tag("tag_1") order by id """

    // Test 1.6.3: Same name for branch and tag (should fail)
    sql """ alter table ${table_name} create branch b1_conflict """
    test {
        sql """ alter table ${table_name} create tag b1_conflict """
        exception
    }

    sql """ alter table ${table_name} create tag t1_conflict """
    test {
        sql """ alter table ${table_name} create branch t1_conflict """
        exception
    }

    // Test 1.6.5: Create branch based on non-existent snapshot
    test {
        sql """ alter table ${table_name} create branch b1_invalid AS OF VERSION 999999 """
        exception
    }

    // Test 2.4.1: Tag name conflict
    sql """ alter table ${table_name} create tag t2_conflict """
    test {
        sql """ alter table ${table_name} create branch t2_conflict """
        exception
    }

    // Test 2.4.2: Tag write operations should fail
    sql """ alter table ${table_name} create tag t2_write_test """
    test {
        sql """ insert into ${table_name}@tag(t2_write_test) values (3, 'c') """
        exception "is a tag, not a branch"
    }

    test {
        sql """ insert overwrite table ${table_name}@tag(t2_write_test) values (3, 'c') """
        exception "is a tag, not a branch"
    }

    // Test 2.4.3: Create tag based on non-existent snapshot
    test {
        sql """ alter table ${table_name} create tag t3_invalid AS OF VERSION 999999 """
        exception
    }

    // Test branch/tag name with quotes
    sql """ alter table ${table_name} create branch "quoted-branch" """
    sql """ alter table ${table_name} create tag "quoted-tag" """

    qt_quoted_branch """ select count(*) from ${table_name}@branch("quoted-branch") """
    qt_quoted_tag """ select count(*) from ${table_name}@tag("quoted-tag") """

    // Test case sensitivity
    sql """ alter table ${table_name} create branch "CaseSensitive" """
    sql """ alter table ${table_name} create tag "CaseSensitiveTag" """

    qt_case_branch """ select count(*) from ${table_name}@branch("CaseSensitive") """
    qt_case_tag """ select count(*) from ${table_name}@tag("CaseSensitiveTag") """

    // Test main branch cannot be deleted
    test {
        sql """ alter table ${table_name} drop branch main """
        exception
    }

    // Test query non-existent branch
    test {
        sql """ select * from ${table_name}@branch(not_exist_branch) """
        exception
    }

    // Test query non-existent tag
    test {
        sql """ select * from ${table_name}@tag(not_exist_tag) """
        exception
    }

    // Test invalid syntax for branch/tag
    test {
        sql """ select * from ${table_name}@branch('name'='invalid_key') """
        exception
    }

    test {
        sql """ select * from ${table_name}@tag('name'='invalid_key') """
        exception
    }

    // Test empty branch/tag name
    test {
        sql """ alter table ${table_name} create branch "" """
        exception
    }

    // Test branch/tag with numbers
    sql """ alter table ${table_name} create branch "branch123" """
    sql """ alter table ${table_name} create tag "tag456" """

    qt_numeric_branch """ select count(*) from ${table_name}@branch("branch123") """
    qt_numeric_tag """ select count(*) from ${table_name}@tag("tag456") """

    // Test mixed case and special characters
    sql """ alter table ${table_name} create branch "Branch_123-Test" """
    sql """ alter table ${table_name} create tag "Tag_456-Test" """

    qt_mixed_branch """ select count(*) from ${table_name}@branch("Branch_123-Test") """
    qt_mixed_tag """ select count(*) from ${table_name}@tag("Tag_456-Test") """
}

