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
    String catalog_name = "iceberg_tag_retention_consistency"

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

    sql """drop database if exists ${catalog_name}.test_db_tag_retention force"""
    sql """create database ${catalog_name}.test_db_tag_retention"""
    sql """ use ${catalog_name}.test_db_tag_retention """

    // Test 1: Tag protection from expire_snapshots - tags should protect their referenced snapshots
    def table_name_expire = "test_tag_expire_snapshots"
    sql """ drop table if exists ${table_name_expire} """
    sql """ create table ${table_name_expire} (id int, name string, data string) """

    // Create multiple snapshots on main branch
    sql """ insert into ${table_name_expire} values (1, 'a', 'snapshot1') """
    sql """ insert into ${table_name_expire} values (2, 'b', 'snapshot2') """
    sql """ insert into ${table_name_expire} values (3, 'c', 'snapshot3') """
    sql """ insert into ${table_name_expire} values (4, 'd', 'snapshot4') """
    sql """ insert into ${table_name_expire} values (5, 'e', 'snapshot5') """

    List<List<Object>> snapshots_expire = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_expire}", "query_type" = "snapshots") order by committed_at; """
    String s_expire_0 = snapshots_expire.get(0)[0]
    String s_expire_1 = snapshots_expire.get(1)[0]
    String s_expire_2 = snapshots_expire.get(2)[0]
    String s_expire_3 = snapshots_expire.get(3)[0]
    String s_expire_4 = snapshots_expire.get(4)[0]

    // Create tags at different snapshot points with retention policies
    sql """ alter table ${table_name_expire} create tag t_early AS OF VERSION ${s_expire_1} RETAIN 30 DAYS """
    sql """ alter table ${table_name_expire} create tag t_middle AS OF VERSION ${s_expire_3} RETAIN 30 DAYS """

    logger.info("Created tags t_early (snapshot ${s_expire_1}) and t_middle (snapshot ${s_expire_3})")

    // Get snapshot count before expire
    def snapshot_count_before_expire = sql """ select count(*) from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_expire}", "query_type" = "snapshots") """
    logger.info("Snapshot count before expire: ${snapshot_count_before_expire[0][0]}")

    // Call expire_snapshots via Spark - should not delete snapshots referenced by tags
    spark_iceberg """CALL demo.system.expire_snapshots(table => 'test_db_tag_retention.${table_name_expire}', older_than => TIMESTAMP '2020-01-01 00:00:00')"""

    // Verify tags are still accessible after expire_snapshots
    qt_expire_tag_early_accessible """ select * from ${table_name_expire}@tag(t_early) order by id """ // Should have 2 records
    qt_expire_tag_middle_accessible """ select * from ${table_name_expire}@tag(t_middle) order by id """ // Should have 4 records

    // Verify tag refs are still in refs table
    def tag_early_ref_after_expire = sql """ select count(*) from ${table_name_expire}\$refs where name = 't_early' """
    assertEquals(tag_early_ref_after_expire[0][0], 1)

    def tag_middle_ref_after_expire = sql """ select count(*) from ${table_name_expire}\$refs where name = 't_middle' """
    assertEquals(tag_middle_ref_after_expire[0][0], 1)

    // Test 2: Multiple tags pointing to the same snapshot
    def table_name_multi_tag = "test_multiple_tags_same_snapshot"
    sql """ drop table if exists ${table_name_multi_tag} """
    sql """ create table ${table_name_multi_tag} (id int, value string) """

    sql """ insert into ${table_name_multi_tag} values (1, 'first') """
    sql """ insert into ${table_name_multi_tag} values (2, 'second') """
    sql """ insert into ${table_name_multi_tag} values (3, 'third') """

    List<List<Object>> snapshots_multi = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_multi_tag}", "query_type" = "snapshots") order by committed_at; """
    String s_multi_1 = snapshots_multi.get(1)[0]

    // Create multiple tags pointing to the same snapshot
    sql """ alter table ${table_name_multi_tag} create tag tag_a AS OF VERSION ${s_multi_1} RETAIN 7 DAYS """
    sql """ alter table ${table_name_multi_tag} create tag tag_b AS OF VERSION ${s_multi_1} RETAIN 15 DAYS """
    sql """ alter table ${table_name_multi_tag} create tag tag_c AS OF VERSION ${s_multi_1} """

    // Verify all tags return the same data
    qt_multi_tag_a """ select * from ${table_name_multi_tag}@tag(tag_a) order by id """ // Should have 1,2
    qt_multi_tag_b """ select * from ${table_name_multi_tag}@tag(tag_b) order by id """ // Should have 1,2
    qt_multi_tag_c """ select * from ${table_name_multi_tag}@tag(tag_c) order by id """ // Should have 1,2

    // Verify all tags are in refs table
    def tag_a_ref = sql """ select count(*) from ${table_name_multi_tag}\$refs where name = 'tag_a' """
    assertEquals(tag_a_ref[0][0], 1)

    def tag_b_ref = sql """ select count(*) from ${table_name_multi_tag}\$refs where name = 'tag_b' """
    assertEquals(tag_b_ref[0][0], 1)

    def tag_c_ref = sql """ select count(*) from ${table_name_multi_tag}\$refs where name = 'tag_c' """
    assertEquals(tag_c_ref[0][0], 1)

    // Test 3: Tag data immutability - tags should not change when main branch is updated
    def table_name_immutable = "test_tag_immutability"
    sql """ drop table if exists ${table_name_immutable} """
    sql """ create table ${table_name_immutable} (id int, description string, timestamp bigint) """

    sql """ insert into ${table_name_immutable} values (1, 'version1', 1000) """
    sql """ insert into ${table_name_immutable} values (2, 'version2', 2000) """

    List<List<Object>> snapshots_imm = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_immutable}", "query_type" = "snapshots") order by committed_at; """
    String s_imm_v1 = snapshots_imm.get(1)[0]

    // Create a tag at this point
    sql """ alter table ${table_name_immutable} create tag v1_baseline AS OF VERSION ${s_imm_v1} """

    // Query the tag immediately
    qt_imm_first_query """ select * from ${table_name_immutable}@tag(v1_baseline) order by id """

    // Make changes to main branch
    sql """ insert into ${table_name_immutable} values (3, 'version3', 3000) """
    sql """ insert into ${table_name_immutable} values (4, 'version4', 4000) """

    // Tag should still return the same data
    qt_imm_after_changes """ select * from ${table_name_immutable}@tag(v1_baseline) order by id """ // Should still have 1,2

    // Main branch should have new data
    qt_imm_main_has_new """ select * from ${table_name_immutable} order by id """ // Should have 1,2,3,4

    // Update main branch with more data
    sql """ insert into ${table_name_immutable} values (5, 'version5', 5000) """

    // Tag should still be unchanged
    qt_imm_unchanged """ select * from ${table_name_immutable}@tag(v1_baseline) order by id """ // Should still have 1,2

    // Test 4: Tag replacement - create or replace tag behavior
    def table_name_replace = "test_tag_replacement"
    sql """ drop table if exists ${table_name_replace} """
    sql """ create table ${table_name_replace} (id int, status string) """

    sql """ insert into ${table_name_replace} values (1, 'pending') """
    List<List<Object>> snapshots_replace = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_replace}", "query_type" = "snapshots") order by committed_at; """
    String s_rep_v1 = snapshots_replace.get(0)[0]

    sql """ insert into ${table_name_replace} values (2, 'approved') """
    List<List<Object>> snapshots_replace_2 = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_replace}", "query_type" = "snapshots") order by committed_at; """
    String s_rep_v2 = snapshots_replace_2.get(1)[0]

    // Create initial tag
    sql """ alter table ${table_name_replace} create tag status_baseline AS OF VERSION ${s_rep_v1} RETAIN 7 DAYS """
    qt_replace_initial """ select * from ${table_name_replace}@tag(status_baseline) order by id """ // Should have 1

    // Replace the tag with a different snapshot
    sql """ alter table ${table_name_replace} create or replace tag status_baseline AS OF VERSION ${s_rep_v2} RETAIN 30 DAYS """
    qt_replace_after """ select * from ${table_name_replace}@tag(status_baseline) order by id """ // Should have 1,2

    // Verify the tag ref still exists (replaced, not deleted)
    def tag_ref_after_replace = sql """ select count(*) from ${table_name_replace}\$refs where name = 'status_baseline' """
    assertEquals(tag_ref_after_replace[0][0], 1)

    // Test 5: Tag with time travel queries and snapshots at different points
    def table_name_time_travel = "test_tag_time_travel"
    sql """ drop table if exists ${table_name_time_travel} """
    sql """ create table ${table_name_time_travel} (id int, version string, created_at string) """

    sql """ insert into ${table_name_time_travel} values (1, 'v1.0', '2024-01-01') """
    sql """ insert into ${table_name_time_travel} values (2, 'v1.0', '2024-01-01') """
    sql """ insert into ${table_name_time_travel} values (3, 'v1.0', '2024-01-01') """

    List<List<Object>> snapshots_tt = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_time_travel}", "query_type" = "snapshots") order by committed_at; """
    String s_tt_1 = snapshots_tt.get(0)[0]
    String s_tt_2 = snapshots_tt.get(1)[0]
    String s_tt_3 = snapshots_tt.get(2)[0]

    sql """ alter table ${table_name_time_travel} create tag release_v1 AS OF VERSION ${s_tt_1} """
    sql """ alter table ${table_name_time_travel} create tag release_v2 AS OF VERSION ${s_tt_2} """
    sql """ alter table ${table_name_time_travel} create tag release_v3 AS OF VERSION ${s_tt_3} """

    // Verify each tag captures the state at that point
    qt_tt_v1 """ select * from ${table_name_time_travel}@tag(release_v1) order by id """ // Should have 1
    qt_tt_v2 """ select * from ${table_name_time_travel}@tag(release_v2) order by id """ // Should have 1,2
    qt_tt_v3 """ select * from ${table_name_time_travel}@tag(release_v3) order by id """ // Should have 1,2,3

    // Add more data to main
    sql """ insert into ${table_name_time_travel} values (4, 'v2.0', '2024-02-01'), (5, 'v2.0', '2024-02-01') """

    // Tags should remain unchanged
    qt_tt_v1_unchanged """ select * from ${table_name_time_travel}@tag(release_v1) order by id """ // Should still have 1
    qt_tt_v3_unchanged """ select * from ${table_name_time_travel}@tag(release_v3) order by id """ // Should still have 1,2,3

    // Main should have all data
    qt_tt_main_all """ select * from ${table_name_time_travel} order by id """ // Should have 1,2,3,4,5

    // Test 6: Tag aggregate queries and consistency
    def table_name_agg = "test_tag_aggregates"
    sql """ drop table if exists ${table_name_agg} """
    sql """ create table ${table_name_agg} (id int, category string, amount int) """

    sql """ insert into ${table_name_agg} values (1, 'A', 100) """
    sql """ insert into ${table_name_agg} values (2, 'B', 200) """
    sql """ insert into ${table_name_agg} values (3, 'A', 150) """
    sql """ insert into ${table_name_agg} values (4, 'C', 300) """

    List<List<Object>> snapshots_agg = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_agg}", "query_type" = "snapshots") order by committed_at; """
    String s_agg_all = snapshots_agg.get(3)[0]

    // Create tag for aggregate queries
    sql """ alter table ${table_name_agg} create tag agg_baseline AS OF VERSION ${s_agg_all} """

    qt_agg_count """ select count(*) from ${table_name_agg}@tag(agg_baseline) """ // Should have 4
    qt_agg_sum """ select sum(amount) from ${table_name_agg}@tag(agg_baseline) """ // Should be 750
    qt_agg_avg """ select avg(amount) from ${table_name_agg}@tag(agg_baseline) """ // Should be 187.5
    qt_agg_group_by """ select category, sum(amount) as total from ${table_name_agg}@tag(agg_baseline) group by category order by category """ // A:250, B:200, C:300

    // Add more data
    sql """ insert into ${table_name_agg} values (5, 'A', 50), (6, 'B', 100) """

    // Tag aggregates should remain unchanged
    qt_agg_count_unchanged """ select count(*) from ${table_name_agg}@tag(agg_baseline) """ // Should still be 4
    qt_agg_sum_unchanged """ select sum(amount) from ${table_name_agg}@tag(agg_baseline) """ // Should still be 750

    // Test 7: Tag and branch interaction
    def table_name_interaction = "test_tag_branch_interaction"
    sql """ drop table if exists ${table_name_interaction} """
    sql """ create table ${table_name_interaction} (id int, name string, source string) """

    sql """ insert into ${table_name_interaction} values (1, 'item1', 'main') """
    sql """ insert into ${table_name_interaction} values (2, 'item2', 'main') """

    List<List<Object>> snapshots_inter = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_interaction}", "query_type" = "snapshots") order by committed_at; """
    String s_inter_baseline = snapshots_inter.get(1)[0]

    // Create a tag for baseline
    sql """ alter table ${table_name_interaction} create tag baseline AS OF VERSION ${s_inter_baseline} """

    // Create a branch and add data
    sql """ alter table ${table_name_interaction} create branch feature_branch """
    sql """ insert into ${table_name_interaction}@branch(feature_branch) values (3, 'item3', 'branch') """
    sql """ insert into ${table_name_interaction}@branch(feature_branch) values (4, 'item4', 'branch') """

    // Tag should still show baseline data
    qt_inter_tag_baseline """ select * from ${table_name_interaction}@tag(baseline) order by id """ // Should have 1,2

    // Branch should have its own data
    qt_inter_branch_data """ select * from ${table_name_interaction}@branch(feature_branch) order by id """ // Should have 1,2,3,4

    // Join between tag and branch
    qt_inter_join """
        select t.id as tag_id, t.name as tag_name, b.id as branch_id, b.name as branch_name
        from ${table_name_interaction}@tag(baseline) t
        full outer join ${table_name_interaction}@branch(feature_branch) b on t.id = b.id
        order by coalesce(t.id, b.id)
    """

    // Test 8: Tag write operations should fail (runtime validation)
    def table_name_write_fail = "test_tag_write_fails"
    sql """ drop table if exists ${table_name_write_fail} """
    sql """ create table ${table_name_write_fail} (id int, value string) """

    sql """ insert into ${table_name_write_fail} values (1, 'data1') """
    List<List<Object>> snapshots_fail = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db_tag_retention.${table_name_write_fail}", "query_type" = "snapshots") order by committed_at; """
    String s_fail = snapshots_fail.get(0)[0]

    // Create a tag
    sql """ alter table ${table_name_write_fail} create tag protected AS OF VERSION ${s_fail} """

    // Attempting to insert into a tag should fail
    test {
        sql """ insert into ${table_name_write_fail}@branch(protected) values (2, 'data2') """
        exception "protected is a tag, not a branch"
    }

    test {
        sql """ insert overwrite table ${table_name_write_fail}@branch(protected) values (2, 'data2') """
        exception "protected is a tag, not a branch"
    }

    // Verify tag is still accessible and unchanged
    qt_write_fail_tag_unchanged """ select * from ${table_name_write_fail}@tag(protected) order by id """ // Should still have 1

}
