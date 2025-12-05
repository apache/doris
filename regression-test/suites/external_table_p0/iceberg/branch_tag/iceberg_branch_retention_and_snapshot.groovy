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

suite("iceberg_branch_retention_and_snapshot", "p0,external,doris,external_docker,external_docker_doris") {
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
    qt_b1_initial """ select * from ${table_name}@branch(b1_retention_count) order by id """ // 2 records

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
    qt_b1_time_initial """ select * from ${table_name}@branch(b1_retention_time) order by id """ // 2 records

    // Test 1.1.4: Combined retention policy
    sql """ alter table ${table_name} create branch b1_combined AS OF VERSION ${s2} RETAIN 30 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS 2 DAYS """
    qt_b1_combined """ select * from ${table_name}@branch(b1_combined) order by id """ // 2 records

    // Test 1.2.1: Branch snapshot independent evolution
    sql """ alter table ${table_name} create branch b2_independent AS OF VERSION ${s1} """
    sql """ alter table ${table_name} create branch b3_independent AS OF VERSION ${s1} """

    sql """ insert into ${table_name}@branch(b2_independent) values (11, 'k') """
    sql """ insert into ${table_name}@branch(b3_independent) values (12, 'l') """

    qt_b2_after_insert """ select * from ${table_name}@branch(b2_independent) order by id """ // Should have 11
    qt_b3_after_insert """ select * from ${table_name}@branch(b3_independent) order by id """ // Should have 12

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
}

