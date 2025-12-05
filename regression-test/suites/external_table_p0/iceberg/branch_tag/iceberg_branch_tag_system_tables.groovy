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

suite("iceberg_branch_tag_system_tables", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_system_tables"

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

    sql """drop database if exists ${catalog_name}.test_db_system force"""
    sql """create database ${catalog_name}.test_db_system"""
    sql """ use ${catalog_name}.test_db_system """

    String table_name = "test_system_tables"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string) """

    sql """ insert into ${table_name} values (1, 'a'), (2, 'b'), (3, 'c') """
    sql """ alter table ${table_name} create branch b1_system """
    sql """ alter table ${table_name} create branch b2_system """
    sql """ alter table ${table_name} create tag t1_system """
    sql """ alter table ${table_name} create tag t2_system """
    sql """ insert into ${table_name}@branch(b1_system) values (4, 'd') """

    // Test 4.1.1: Query all refs information
    qt_refs_all """ select * from ${table_name}\$refs order by name """

    // Test 4.1.2: Filter branch information
    qt_refs_branches """ select * from ${table_name}\$refs where type = 'BRANCH' order by name """

    // Test 4.1.3: Filter tag information
    qt_refs_tags """ select * from ${table_name}\$refs where type = 'TAG' order by name """

    // Test 4.1.4: Query specific ref information
    qt_refs_b1 """ select * from ${table_name}\$refs where name = 'b1_system' """

    // Test 4.1.5: Refs information sorting
    qt_refs_sorted """ select * from ${table_name}\$refs order by name """

    // Test 4.1.6: Refs and snapshots join query
    qt_refs_snapshots_join """ 
        select r.name, r.type, r.snapshot_id, s.committed_at
        from ${table_name}\$refs r
        join ${table_name}\$snapshots s on r.snapshot_id = s.snapshot_id
        order by r.name
    """

    // Test 4.2.1: $snapshots table query
    qt_snapshots_all """ select snapshot_id, committed_at from ${table_name}\$snapshots order by committed_at """

    // Test 4.2.2: $files table query
    qt_files_all """ select * from ${table_name}\$files limit 10 """

    // Test refs count verification
    qt_refs_count """ select count(*) from ${table_name}\$refs """
    qt_refs_branch_count """ select count(*) from ${table_name}\$refs where type = 'BRANCH' """
    qt_refs_tag_count """ select count(*) from ${table_name}\$refs where type = 'TAG' """

    // Test refs with snapshot details
    qt_refs_with_details """ 
        select 
            r.name,
            r.type,
            r.snapshot_id,
            s.operation,
            s.summary
        from ${table_name}\$refs r
        left join ${table_name}\$snapshots s on r.snapshot_id = s.snapshot_id
        order by r.name
    """

    // Test query refs after creating more branches and tags
    sql """ alter table ${table_name} create branch b3_system """
    sql """ alter table ${table_name} create tag t3_system """
    sql """ insert into ${table_name}@branch(b3_system) values (5, 'e') """

    qt_refs_after_more """ select count(*) from ${table_name}\$refs """
    qt_refs_branches_after """ select count(*) from ${table_name}\$refs where type = 'BRANCH' """
    qt_refs_tags_after """ select count(*) from ${table_name}\$refs where type = 'TAG' """
}

