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

    // Test 1: expire_snapshots verification - snapshots referenced by branches/tags should be protected
    def table_name_expire = "test_expire_snapshots_branch"
    sql """ drop table if exists ${table_name_expire} """
    sql """ create table ${table_name_expire} (id int, name string) """

    // Create multiple snapshots on main branch
    sql """ insert into ${table_name_expire} values (1, 'a') """
    sql """ insert into ${table_name_expire} values (2, 'b') """
    sql """ insert into ${table_name_expire} values (3, 'c') """
    sql """ insert into ${table_name_expire} values (4, 'd') """
    sql """ insert into ${table_name_expire} values (5, 'e') """

    List<List<Object>> snapshots_expire = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_expire}", "query_type" = "snapshots") order by committed_at; """
    String s_expire_0 = snapshots_expire.get(0)[0]
    String s_expire_1 = snapshots_expire.get(1)[0]
    String s_expire_2 = snapshots_expire.get(2)[0]
    String s_expire_3 = snapshots_expire.get(3)[0]
    String s_expire_4 = snapshots_expire.get(4)[0]

    // Create a branch with snapshot retention policy
    sql """ alter table ${table_name_expire} create branch b_expire_test AS OF VERSION ${s_expire_2} RETAIN 30 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS """

    // Write multiple snapshots to the branch
    sql """ insert into ${table_name_expire}@branch(b_expire_test) values (6, 'f') """
    sql """ insert into ${table_name_expire}@branch(b_expire_test) values (7, 'g') """
    sql """ insert into ${table_name_expire}@branch(b_expire_test) values (8, 'h') """
    sql """ insert into ${table_name_expire}@branch(b_expire_test) values (9, 'i') """
    sql """ insert into ${table_name_expire}@branch(b_expire_test) values (10, 'j') """

    // Get the current snapshot count before expire
    def snapshot_count_before_expire = sql """ select count(*) from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_expire}", "query_type" = "snapshots") """
    logger.info("Snapshot count before expire: ${snapshot_count_before_expire[0][0]}")

    // Get branch ref snapshot
    def branch_ref_snapshot = sql """ select snapshot_id from ${table_name_expire}\$refs where name = 'b_expire_test' """

    // Create tags to protect additional snapshots
    sql """ alter table ${table_name_expire} create tag t_expire_protect AS OF VERSION ${s_expire_1} """

    // Call expire_snapshots via Doris - should not delete snapshots referenced by branch/tag
    // Using a timestamp that would expire old snapshots but not those referenced by branch/tag
    sql """
        ALTER TABLE ${catalog_name}.test_db_retention.${table_name_expire}
        EXECUTE expire_snapshots("older_than" = "2020-01-01T00:00:00")
    """

    // Verify snapshots are still accessible after expire_snapshots
    qt_expire_branch_still_accessible """ select count(*) from ${table_name_expire}@branch(b_expire_test) """ // Should still have data
    qt_expire_tag_still_accessible """ select * from ${table_name_expire}@tag(t_expire_protect) order by id """ // Should have 2 records

    // Verify the branch ref is still in refs table
    def branch_ref_after_expire = sql """ select count(*) from ${table_name_expire}\$refs where name = 'b_expire_test' """
    assertEquals(branch_ref_after_expire[0][0], 1)

    // Verify the tag ref is still in refs table
    def tag_ref_after_expire = sql """ select count(*) from ${table_name_expire}\$refs where name = 't_expire_protect' """
    assertEquals(tag_ref_after_expire[0][0], 1)

    // Test 3: expire_snapshots with snapshot retention count policy
    def table_name_retain_count = "test_expire_retain_count"
    sql """ drop table if exists ${table_name_retain_count} """
    sql """ create table ${table_name_retain_count} (id int, name string) """

    sql """ insert into ${table_name_retain_count} values (1, 'a') """
    List<List<Object>> snapshots_retain = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_retain_count}", "query_type" = "snapshots") order by committed_at; """
    def s_retain_0 = snapshots_retain[0][0].toString()

    // Create branch with snapshot retention count of 3
    sql """ alter table ${table_name_retain_count} create branch b_retain_count AS OF VERSION ${s_retain_0} RETAIN 30 DAYS WITH SNAPSHOT RETENTION 3 SNAPSHOTS """

    // Write 5 snapshots to the branch (exceeding retention count of 3)
    sql """ insert into ${table_name_retain_count}@branch(b_retain_count) values (2, 'b') """
    sql """ insert into ${table_name_retain_count}@branch(b_retain_count) values (3, 'c') """
    sql """ insert into ${table_name_retain_count}@branch(b_retain_count) values (4, 'd') """
    sql """ insert into ${table_name_retain_count}@branch(b_retain_count) values (5, 'e') """
    sql """ insert into ${table_name_retain_count}@branch(b_retain_count) values (6, 'f') """

    // Get current snapshot id on branch
    def branch_snapshot_id = sql """ select snapshot_id from ${table_name_retain_count}\$refs where name = 'b_retain_count' """

    // Count snapshots before expire
    def snapshot_count_retain_before = sql """ select count(*) from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_retain_count}", "query_type" = "snapshots") """

    // Call expire_snapshots - older snapshots beyond retention count may be expired, but branch snapshot should be protected
    sql """
        ALTER TABLE ${catalog_name}.test_db_retention.${table_name_retain_count}
        EXECUTE expire_snapshots("older_than" = "2020-01-01T00:00:00")
    """

    // Verify branch is still accessible and has data
    qt_retain_count_branch_accessible """ select count(*) from ${table_name_retain_count}@branch(b_retain_count) """ // Should have data
    qt_retain_count_branch_data """ select count(*) from ${table_name_retain_count}@branch(b_retain_count) where id > 0 """ // Should have multiple records

    // Verify branch ref still exists
    def branch_ref_retain_after = sql """ select count(*) from ${table_name_retain_count}\$refs where name = 'b_retain_count' """
    assertEquals(branch_ref_retain_after[0][0], 1)

    // Test 4: Verify that old unreferenced snapshots can be expired
    def table_name_unref = "test_expire_unreferenced"
    sql """ drop table if exists ${table_name_unref} """
    sql """ create table ${table_name_unref} (id int, name string) """

    // Create snapshots
    sql """ insert into ${table_name_unref} values (1, 'old') """
    sql """ insert into ${table_name_unref} values (2, 'old2') """
    sql """ insert into ${table_name_unref} values (3, 'new') """

    List<List<Object>> snapshots_unref = sql """ select snapshot_id, committed_at from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_unref}", "query_type" = "snapshots") order by committed_at; """
    def old_snapshot_id = snapshots_unref[0][0]

    // Create a tag pointing to the newest snapshot (not the old ones)
    List<List<Object>> latest_snapshot = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_unref}", "query_type" = "snapshots") order by committed_at desc limit 1; """
    def latest_snap_id = latest_snapshot[0][0]
    sql """ alter table ${table_name_unref} create tag t_latest AS OF VERSION ${latest_snap_id} """

    // Verify tag has all data before expiration
    qt_unref_tag_before_expire """ select * from ${table_name_unref}@tag(t_latest) order by id """ // Should have 1, old2, 3 rows

    // Count snapshots before expire
    def snapshot_count_unref_before = sql """ select count(*) from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_unref}", "query_type" = "snapshots") """
    logger.info("Snapshot count before expire: ${snapshot_count_unref_before[0][0]}")

    // Call expire_snapshots - old unreferenced snapshots should be expired
    sql """
        ALTER TABLE ${catalog_name}.test_db_retention.${table_name_unref}
        EXECUTE expire_snapshots("retain_last" = "1")
    """

    // Count snapshots after expire
    def snapshot_count_unref_after = sql """ select count(*) from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_unref}", "query_type" = "snapshots") """

    // Refresh catalog to ensure we see the latest state after expire_snapshots
    sql """refresh catalog ${catalog_name}"""

    // Verify that at least the latest snapshot (referenced by tag) still exists
    qt_unref_tag_accessible """ select * from ${table_name_unref}@tag(t_latest) order by id """ // Should have data

    // Verify old snapshot is no longer accessible if it was expired
    def old_snapshot_exists = sql """ select count(*) from iceberg_meta("table" = "${catalog_name}.test_db_retention.${table_name_unref}", "query_type" = "snapshots") where snapshot_id = '${old_snapshot_id}' """
    logger.info("Old snapshot exists after expire: ${old_snapshot_exists[0][0]}")

    // The tag-protected snapshot should still be in refs
    def tag_ref_unref_after = sql """ select count(*) from ${table_name_unref}\$refs where name = 't_latest' """
    assertEquals(tag_ref_unref_after[0][0], 1)

}
