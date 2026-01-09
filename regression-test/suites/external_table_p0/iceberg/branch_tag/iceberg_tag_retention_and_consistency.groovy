package iceberg.branch_tag
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

suite("iceberg_tag_retention_and_consistency", "p0,external,doris,external_docker,external_docker_doris,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_tag_retention"

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

    sql """drop database if exists ${catalog_name}.test_db_tag force"""
    sql """create database ${catalog_name}.test_db_tag"""
    sql """ use ${catalog_name}.test_db_tag """

    String table_name = "test_tag_retention"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """

    sql """ insert into ${table_name} values (1, 'a') """
    sql """ insert into ${table_name} values (2, 'b') """
    sql """ insert into ${table_name} values (3, 'c') """

    List<List<Object>> snapshots = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag.${table_name}", "query_type" = "snapshots") order by committed_at; """
    String s0 = snapshots.get(0)[0]
    String s1 = snapshots.get(1)[0]
    String s2 = snapshots.get(2)[0]

    // Test 2.1.1: Tag retention time verification
    sql """ alter table ${table_name} create tag t1_retention AS OF VERSION ${s1} RETAIN 1 DAYS """
    qt_t1_initial """ select * from ${table_name}@tag(t1_retention) order by id """ // Should have 1,2
    // Test 2.1.2: Tag retention time update
    sql """ alter table ${table_name} create or replace tag t1_retention RETAIN 30 DAYS """
    qt_t1_after_update """ select * from ${table_name}@tag(t1_retention) order by id """ // Should still have same data

    // Test 2.2.1: Tag data immutability
    sql """ alter table ${table_name} create tag t2_immutable AS OF VERSION ${s1} """
    qt_t2_first_query """ select * from ${table_name}@tag(t2_immutable) order by id """ // Should have 1,2

    sql """ insert into ${table_name} values (4, 'd'), (5, 'e') """
    sql """ insert into ${table_name} values (6, 'f') """

    qt_t2_second_query """ select * from ${table_name}@tag(t2_immutable) order by id """ // Should still have 1,2
    qt_t2_third_query """ select * from ${table_name}@tag(t2_immutable) order by id """ // Should still have 1,2
    qt_main_has_new_data """ select * from ${table_name} order by id """ // Main should have new data

    // Test 2.2.2: Multiple tags pointing to same snapshot
    sql """ alter table ${table_name} create tag t3_same AS OF VERSION ${s1} """
    sql """ alter table ${table_name} create tag t4_same AS OF VERSION ${s1} """

    qt_t3_data """ select * from ${table_name}@tag(t3_same) order by id """ // Should have 1,2
    qt_t4_data """ select * from ${table_name}@tag(t4_same) order by id """ // Should have 1,2

    // Verify they return same data
    qt_t3_t4_same """ 
        select count(*) from (
            select * from ${table_name}@tag(t3_same)
            union all
            select * from ${table_name}@tag(t4_same)
        ) t 
    """

    // Test 2.2.3: Tag replacement verification
    sql """ alter table ${table_name} create tag t5_replace AS OF VERSION ${s0} """
    qt_t5_before_replace """ select * from ${table_name}@tag(t5_replace) order by id """ // Should have 1

    sql """ alter table ${table_name} create or replace tag t5_replace AS OF VERSION ${s2} """
    qt_t5_after_replace """ select * from ${table_name}@tag(t5_replace) order by id """ // Should have 1,2,3

    // Test 2.3.1: Tag aggregate queries
    sql """ alter table ${table_name} create tag t6_agg AS OF VERSION ${s2} """
    qt_tag_count """ select count(*) from ${table_name}@tag(t6_agg) """
    qt_tag_sum """ select sum(id) from ${table_name}@tag(t6_agg) """
    qt_tag_avg """ select avg(id) from ${table_name}@tag(t6_agg) """

    // Test 2.3.2: Tag and branch join queries
    sql """ alter table ${table_name} create branch b1_tag """
    sql """ insert into ${table_name}@branch(b1_tag) values (7, 'g'), (8, 'h') """

    qt_tag_branch_join """ 
        select t.id as tag_id, t.name as tag_name, b.id as branch_id, b.name as branch_name
        from ${table_name}@tag(t6_agg) t
        full outer join ${table_name}@branch(b1_tag) b on t.id = b.id
        order by coalesce(t.id, b.id)
    """

    // Test 2.3.3: Tag time travel queries
    sql """ alter table ${table_name} create tag t7_v1 AS OF VERSION ${s0} """
    sql """ alter table ${table_name} create tag t8_v2 AS OF VERSION ${s1} """
    sql """ alter table ${table_name} create tag t9_v3 AS OF VERSION ${s2} """

    qt_tag_v1 """ select * from ${table_name}@tag(t7_v1) order by id """ // Should have 1
    qt_tag_v2 """ select * from ${table_name}@tag(t8_v2) order by id """ // Should have 1,2
    qt_tag_v3 """ select * from ${table_name}@tag(t9_v3) order by id """ // Should have 1,2,3

    // Test tag write operations should fail
    test {
        sql """ insert into ${table_name}@tag(t6_agg) values (9, 'i') """
        exception "is a tag, not a branch"
    }

    test {
        sql """ insert overwrite table ${table_name}@tag(t6_agg) values (9, 'i') """
        exception "is a tag, not a branch"
    }
}

