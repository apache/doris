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

suite("iceberg_branch_retention_and_snapshot", "p0,external,doris,external_docker,external_docker_doris,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_retention_snapshot"

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

    sql """drop database if exists ${catalog_name}.test_db_retention force"""
    sql """create database ${catalog_name}.test_db_retention"""
    sql """ use ${catalog_name}.test_db_retention """

    String table_name = "test_branch_retention"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """

    // Prepare data and snapshots
    sql """ insert into ${table_name} values (1, 'a') """
    sql """ insert into ${table_name} values (2, 'b') """
    sql """ insert into ${table_name} values (3, 'c') """
    sql """ insert into ${table_name} values (4, 'd') """
    sql """ insert into ${table_name} values (5, 'e') """

    List<List<Object>> snapshots = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name}", "query_type" = "snapshots") order by committed_at; """
    String s0 = snapshots.get(0)[0]
    String s1 = snapshots.get(1)[0]
    String s2 = snapshots.get(2)[0]
    String s3 = snapshots.get(3)[0]
    String s4 = snapshots.get(4)[0]

    // Test 1.1.2: Snapshot retention count verification
    sql """ alter table ${table_name} create branch b1_retention_count AS OF VERSION ${s2} RETAIN 30 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS """
    qt_b1_initial """ select * from ${table_name}@branch(b1_retention_count) order by id """ // 3 records
    def snapshot_id1_refs_b1 = sql """ select snapshot_id from ${table_name}\$refs where name = 'b1_retention_count' """
    assertEquals(snapshot_id1_refs_b1[0][0].toString(), s2)

    // Write multiple snapshots to branch
    sql """ insert into ${table_name}@branch(b1_retention_count) values (6, 'f') """
    sql """ insert into ${table_name}@branch(b1_retention_count) values (7, 'g') """
    sql """ insert into ${table_name}@branch(b1_retention_count) values (8, 'h') """
    sql """ insert into ${table_name}@branch(b1_retention_count) values (9, 'i') """
    sql """ insert into ${table_name}@branch(b1_retention_count) values (10, 'j') """

    // Verify branch still has data
    qt_b1_after_writes """ select count(*) from ${table_name}@branch(b1_retention_count) """ // Should have more than 2 records

    // Test 1.1.3: Snapshot retention time verification
    sql """ alter table ${table_name} create branch b1_retention_time AS OF VERSION ${s2} RETAIN 30 DAYS WITH SNAPSHOT RETENTION 1 DAYS """
    qt_b1_time_initial """ select * from ${table_name}@branch(b1_retention_time) order by id """ // 3 records

    // Test 1.1.4: Combined retention policy
    sql """ alter table ${table_name} create branch b1_combined AS OF VERSION ${s2} RETAIN 30 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS 2 DAYS """
    qt_b1_combined """ select * from ${table_name}@branch(b1_combined) order by id """ // 3 records

    // Test 1.2.1: Branch snapshot independent evolution
    sql """ alter table ${table_name} create branch b2_independent AS OF VERSION ${s1} """
    sql """ alter table ${table_name} create branch b3_independent AS OF VERSION ${s1} """

    def snapshot_id1_refs_b2 = sql """ select snapshot_id from ${table_name}\$refs where name = 'b2_independent' """
    assertEquals(snapshot_id1_refs_b2[0][0].toString(), s1)


    sql """ insert into ${table_name}@branch(b2_independent) values (11, 'k') """
    sql """ insert into ${table_name}@branch(b3_independent) values (12, 'l') """

    qt_b2_after_insert """ select * from ${table_name}@branch(b2_independent) order by id """ // Should have 11
    qt_b3_after_insert """ select * from ${table_name}@branch(b3_independent) order by id """ // Should have 12

    def snapshot_id1_refs_b2_after = sql """ select snapshot_id from ${table_name}\$refs where name = 'b2_independent' """
    def snapshot_id1_refs_b3_after = sql """ select snapshot_id from ${table_name}\$refs where name = 'b3_independent' """
    def snapshot_id1_parent_id = sql """select parent_id from ${table_name}\$snapshots where snapshot_id = '${snapshot_id1_refs_b2_after[0][0]}'"""
    def snapshot_id2_parent_id = sql """select parent_id from ${table_name}\$snapshots where snapshot_id = '${snapshot_id1_refs_b3_after[0][0]}'"""
    assertEquals(snapshot_id1_parent_id[0][0].toString(), snapshot_id2_parent_id[0][0].toString())
    assertEquals(snapshot_id1_parent_id[0][0].toString(), s1)

    // Verify branches are independent
    qt_b2_count """ select count(*) from ${table_name}@branch(b2_independent) """
    qt_b3_count """ select count(*) from ${table_name}@branch(b3_independent) """

    // Test 1.2.3: Branch snapshot rollback
    sql """ alter table ${table_name} create branch b4_rollback """
    sql """ insert into ${table_name}@branch(b4_rollback) values (13, 'm') """
    sql """ insert into ${table_name}@branch(b4_rollback) values (14, 'n') """
    sql """ insert into ${table_name}@branch(b4_rollback) values (15, 'o') """

    qt_b4_before_rollback """ select count(*) from ${table_name}@branch(b4_rollback) """ // Should have more records

    // Rollback to initial state
    sql """ alter table ${table_name} create or replace branch b4_rollback AS OF VERSION ${s0} """
    qt_b4_after_rollback """ select * from ${table_name}@branch(b4_rollback) order by id """ // Should be empty or initial state

    // Test branch retention with different time units
    sql """ alter table ${table_name} create branch b5_hours RETAIN 2 HOURS """
    sql """ alter table ${table_name} create branch b6_minutes RETAIN 30 MINUTES """
    sql """ alter table ${table_name} create branch b7_days RETAIN 1 DAYS """

    qt_b5_hours """ select count(*) from ${table_name}@branch(b5_hours) """
    qt_b6_minutes """ select count(*) from ${table_name}@branch(b6_minutes) """
    qt_b7_days """ select count(*) from ${table_name}@branch(b7_days) """

    def table_name2 = "test_branch_retention_2"
    sql """ drop table if exists ${table_name2} """
    sql """ create table ${table_name2} (id int, name string) """
    def b1 = "branch_retention_2_b1"
    def b2 = "branch_retention_2_b2"
    def t1 = "branch_retention_2_t1"
    sql """insert into ${table_name2} values (100, 'base') """
    List<List<Object>> snapshots2 = sql """ select snapshot_id from ${table_name2}\$snapshots order by committed_at; """
    def snapshot2_0 = snapshots2[0][0].toString()
    sql """alter table ${table_name2} create branch ${b1} AS OF VERSION ${snapshot2_0} RETAIN 2 MINUTES WITH SNAPSHOT RETENTION 3 SNAPSHOTS 1 MINUTES"""
    def snapshot_id_b1 = sql """ select snapshot_id from ${table_name2}\$refs where name = '${b1}' """
    assertEquals(snapshot_id_b1[0][0].toString(), snapshot2_0)
    sql """insert into ${table_name2}@branch(${b1}) values (1, 'x'); """
    sql """insert into ${table_name2}@branch(${b1}) values (2, 'y'); """
    sql """insert into ${table_name2}@branch(${b1}) values (3, 'z'); """
    sql """insert into ${table_name2}@branch(${b1}) values (4, 'w'); """
    qt_branch2_b1_start """ select * from ${table_name2}@branch(${b1}) """ // Should have 3 records
    def snapshot_id_b1_1 = sql """ select snapshot_id from ${table_name2}\$refs where name = '${b1}' """
    sql """alter table ${table_name2} create branch ${b2} AS OF VERSION ${snapshot_id_b1_1[0][0]}"""
    sql """alter table ${table_name2} create tag ${t1} AS OF VERSION ${snapshot_id_b1_1[0][0]}"""
    sleep(70000) // Sleep for 70 seconds to exceed retention time
    sql """select * from ${table_name2}@branch(${b2}) """
    sql """select * from ${table_name2}@tag(${t1}) """

    sleep(60000) // Sleep for another 60 seconds to exceed retaintion time
    sql """select * from ${table_name2}@branch(${b1}) """
    qt_branch2_b1_end """ select count(*) from ${table_name2}\$refs where name='${b1}';""" // Shouldhave 1 record


}

