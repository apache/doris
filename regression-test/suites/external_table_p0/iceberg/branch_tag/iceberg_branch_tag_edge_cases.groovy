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

suite("iceberg_branch_tag_edge_cases", "p0,external,doris,external_docker,external_docker_doris,branch_tag") {
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
    def snapshots = sql """ select snapshot_id from ${table_name}\$snapshots order by committed_at desc limit 1 """
    def snapshot_id = snapshots[0][0]


    // Test 1.6.1: Special characters in branch/tag names
    sql """ alter table ${table_name} create branch `branch-1` """
    sql """ alter table ${table_name} create branch `branch_1` """
    sql """ alter table ${table_name} create tag `tag-1` """
    sql """ alter table ${table_name} create tag `tag_1` """

    qt_branch_hyphen """ select * from ${table_name}@branch(`branch-1`) order by id """
    qt_branch_underscore """ select * from ${table_name}@branch(`branch_1`) order by id """
    qt_tag_hyphen """ select * from ${table_name}@tag(`tag-1`) order by id """
    qt_tag_underscore """ select * from ${table_name}@tag(`tag_1`) order by id """

    // Test 1.6.3: Same name for branch and tag (should fail)
    sql """ alter table ${table_name} create branch b1_conflict """
    test {
        sql """ alter table ${table_name} create tag b1_conflict """
        exception "Ref b1_conflict already exists"
    }

    sql """ alter table ${table_name} create tag t1_conflict """
    test {
        sql """ alter table ${table_name} create branch t1_conflict """
        exception "Ref t1_conflict already exists"
    }

    // Test 1.6.5: Create branch based on non-existent snapshot
    test {
        sql """ alter table ${table_name} create branch b1_invalid AS OF VERSION 999999 """
        exception "Cannot set b1_invalid to unknown snapshot: 99999"
    }

    // Test 2.4.1: Tag name conflict
    sql """ alter table ${table_name} create tag t2_conflict """
    test {
        sql """ alter table ${table_name} create branch t2_conflict """
        exception "Ref t2_conflict already exists"
    }

    // Test 2.4.2: Tag write operations should fail
    sql """ alter table ${table_name} create tag t2_write_test """
    test {
        sql """ insert into ${table_name}@tag(t2_write_test) values (3, 'c') """
        exception "mismatched input 'tag' expecting 'BRANCH'"
    }

    test {
        sql """ insert overwrite table ${table_name}@tag(t2_write_test) values (3, 'c') """
        exception "mismatched input 'tag' expecting 'BRANCH'"
    }

    // Test 2.4.3: Create tag based on non-existent snapshot
    test {
        sql """ alter table ${table_name} create tag t3_invalid AS OF VERSION 999999 """
        exception "Cannot set t3_invalid to unknown snapshot: 999999"
    }

    // Test branch/tag name with quotes
    /*
    sql """ alter table ${table_name} create branch "quoted-branch" """
    sql """ alter table ${table_name} create tag "quoted-tag" """

    qt_quoted_branch """ select count(*) from ${table_name}@branch("quoted-branch") """
    qt_quoted_tag """ select count(*) from ${table_name}@tag("quoted-tag") """
     */

    // Test case sensitivity
    sql """ alter table ${table_name} create branch `CaseSensitive` """
    sql """ alter table ${table_name} create tag `CaseSensitiveTag` """

    qt_case_branch """ select count(*) from ${table_name}@branch(`CaseSensitive`) """
    qt_case_tag """ select count(*) from ${table_name}@tag(`CaseSensitiveTag`) """

    // Test main branch cannot be deleted
    test {
        sql """ alter table ${table_name} drop branch main """
        exception "Cannot remove main branc"
    }

    // Test query non-existent branch
    test {
        sql """ select * from ${table_name}@branch(not_exist_branch) """
        exception "does not have branch named not_exist_branc"
    }

    // Test query non-existent tag
    test {
        sql """ select * from ${table_name}@tag(not_exist_tag) """
        exception "does not have tag named not_exist_tag"
    }

    // Test invalid syntax for branch/tag
    test {
        sql """ select * from ${table_name}@branch('name'='invalid_key') """
        exception "does not have branch named invalid_key"
    }

    test {
        sql """ select * from ${table_name}@tag('name'='invalid_key') """
        exception "does not have tag named invalid_key"
    }

    // Test empty branch/tag name
    /*
    test {
        sql """ alter table ${table_name} create branch `` """
        exception
    }
    */

    // Test branch/tag with numbers
    sql """ alter table ${table_name} create branch `branch123` """
    sql """ alter table ${table_name} create tag `tag456` """

    qt_numeric_branch """ select count(*) from ${table_name}@branch(`branch123`) """
    qt_numeric_tag """ select count(*) from ${table_name}@tag(`tag456`) """

    // Test mixed case and special characters
    sql """ alter table ${table_name} create branch `Branch_123-Test` """
    sql """ alter table ${table_name} create tag `Tag_456-Test` """

    qt_mixed_branch """ select count(*) from ${table_name}@branch(`Branch_123-Test`) """
    qt_mixed_tag """ select count(*) from ${table_name}@tag(`Tag_456-Test`) """

    // Test branch/tag creation with retention and snapshot retention edge cases
    test {
        sql """ alter table ${table_name} create branch b1_invalid_unit as of version ${snapshot_id} retain 2 SECONDS """
        exception "mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'"
    }
    test {
        sql """ alter table ${table_name} create branch b2_invalid_unit as of version ${snapshot_id} with snapshot retention 3 SNAPSHOTS 1 SECONDS """
        exception "mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'"
    }

    test {
        sql """ alter table ${table_name} create branch b3_invalid_num as of version ${snapshot_id} retain 0 MINUTES """
        exception "Max snapshot age must be greater than 0 ms"
    }
    test {
        sql """ alter table ${table_name} create branch b4_invalid_num as of version ${snapshot_id} retain -1 MINUTES """
        exception "expecting INTEGER_VALUE"
    }
    test {
        sql """ alter table ${table_name} create branch b5_invalid_num as of version ${snapshot_id} retain xxx MINUTES """
        exception "expecting INTEGER_VALUE"
    }
    test {
        sql """ alter table ${table_name} create branch b6_invalid_num as of version ${snapshot_id} with snapshot retention 0 SNAPSHOTS 1 MINUTES """
        exception "Min snapshots to keep must be greater than 0"
    }
    test {
        sql """ alter table ${table_name} create branch b7_invalid_num as of version ${snapshot_id} with snapshot retention -1 SNAPSHOTS 1 MINUTES """
        exception "no viable alternative at input 'with snapshot retention -'"
    }
//    test {
//        sql """ alter table ${table_name} create branch b8_invalid_num as of version ${snapshot_id} with snapshot retention xxx SNAPSHOTS 1 MINUTES """
//        exception "Snapshot count must be positive integer"
//    }
    test {
        sql """ alter table ${table_name} create branch b9_invalid_num as of version ${snapshot_id} with snapshot retention 3 SNAPSHOTS 0 MINUTES """
        exception "Max reference age must be greater than 0"
    }
//    test {
//        sql """ alter table ${table_name} create branch b10_invalid_num as of version ${snapshot_id} with snapshot retention 3 SNAPSHOTS -1 MINUTES """
//        exception "Retention value must be positive integer"
//    }
//    test {
//        sql """ alter table ${table_name} create branch b11_invalid_num as of version ${snapshot_id} with snapshot retention 3 SNAPSHOTS xxx MINUTES """
//        exception "Retention value must be positive integer"
//    }

    test {
        sql """ alter table ${table_name} create tag t1_invalid_unit as of version ${snapshot_id} retain 1 SECONDS """
        exception "mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'"
    }
    test {
        sql """ alter table ${table_name} create tag t3_invalid_num as of version ${snapshot_id} retain 0 MINUTES """
        exception "Max reference age must be greater than 0"
    }
    test {
        sql """ alter table ${table_name} create tag t4_invalid_num as of version ${snapshot_id} retain -1 MINUTES """
        exception "extraneous input '-' expecting INTEGER_VALUE"
    }
    test {
        sql """ alter table ${table_name} create tag t5_invalid_num as of version ${snapshot_id} retain xxx MINUTES """
        exception "mismatched input 'xxx' expecting INTEGER_VALUE"
    }
}
