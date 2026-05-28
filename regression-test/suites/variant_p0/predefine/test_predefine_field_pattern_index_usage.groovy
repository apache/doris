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

suite("test_predefine_field_pattern_index_usage", "p0") {
    def timeout = 60000
    def delta_time = 1000
    def wait_for_latest_op_on_table_finish = { tableName, OpTimeout ->
        def alter_res = "null"
        def useTime = 0
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if (alter_res.contains("FINISHED")) {
                sleep(3000)
                logger.info(tableName + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def queryAndCheckInvertedIndex = { String tag, String sqlQuery, int expectedCount ->
        def queryResult = null
        sql """ sync """
        profile(tag) {
            run {
                queryResult = sql """ /* ${tag} */ ${sqlQuery} """
            }
            check { profileString, exception ->
                if (exception != null) {
                    throw exception
                }
                assertEquals(1, queryResult.size())
                assertEquals(expectedCount.toString(), queryResult[0][0].toString())
                def matcher = (profileString =~ /RowsInvertedIndexFiltered:\s+[^\n\(]*\((\d+)\)/)
                int filteredRows = 0
                boolean foundMetric = false
                while (matcher.find()) {
                    foundMetric = true
                    filteredRows += matcher.group(1).toInteger()
                }
                if (!foundMetric) {
                    matcher = (profileString =~ /RowsInvertedIndexFiltered:\s+(\d+)/)
                    while (matcher.find()) {
                        foundMetric = true
                        filteredRows += matcher.group(1).toInteger()
                    }
                }
                if (foundMetric) {
                    assertTrue(filteredRows > 0, "Expected inverted index to filter rows for ${sqlQuery}, profile: ${profileString}")
                } else {
                    assertTrue(profileString.contains("PhysicalStorageLayerAggregate")
                                    && profileString.contains("pushDownAggOp=COUNT_ON_MATCH")
                                    && profileString.contains("MATCH_ANY"),
                            "Expected inverted index pushdown for ${sqlQuery}, profile: ${profileString}")
                    logger.info("RowsInvertedIndexFiltered is not reported for ${tag}, checked COUNT_ON_MATCH pushdown instead")
                }
            }
        }
    }

    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_max_subcolumns_count = 10 """
    sql """ set default_variant_max_sparse_column_statistics_size = 10 """
    sql """ set default_variant_sparse_hash_shard_count = 10 """
    sql """ set enable_add_index_for_new_data = true """
    sql """ set enable_profile = true """
    sql """ set profile_level = 2 """

    sql """ DROP TABLE IF EXISTS test_ddl_field_pattern_index_verify """
    sql """ CREATE TABLE test_ddl_field_pattern_index_verify (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'ab' : string,
            properties("variant_max_subcolumns_count" = "10")
        > NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true") """
    sql """ alter table test_ddl_field_pattern_index_verify modify column var variant<
                MATCH_NAME 'ab' : string,
                MATCH_NAME 'content' : string,
                properties("variant_max_subcolumns_count" = "10")
            > NULL """
    wait_for_latest_op_on_table_finish("test_ddl_field_pattern_index_verify", timeout)
    sql """ create index idx_content_verify on test_ddl_field_pattern_index_verify (var) using inverted
            properties("field_pattern"="content", "parser"="english", "support_phrase" = "true") """
    sql """ insert into test_ddl_field_pattern_index_verify values
            (1, '{"content":"needle alpha", "ab":"alpha"}'),
            (2, '{"content":"other beta", "ab":"beta"}'),
            (3, '{"content":"other gamma", "ab":"gamma"}') """
    queryAndCheckInvertedIndex(
            "test_ddl_field_pattern_index_verify_${System.currentTimeMillis()}",
            """ select /*+ SET_VAR(experimental_enable_parallel_scan=false,
                       inverted_index_skip_threshold=0,
                       enable_common_expr_pushdown_for_inverted_index=true,
                       enable_common_expr_pushdown=true,
                       enable_match_without_inverted_index=false) */
                       count() from test_ddl_field_pattern_index_verify where var['content'] match 'needle' """,
            1)
    def indexedMatchResult = sql """ select id from test_ddl_field_pattern_index_verify
            where var['content'] match 'needle' order by id """
    assertEquals(1, indexedMatchResult.size())
    assertEquals("1", indexedMatchResult[0][0].toString())

    sql """ DROP TABLE IF EXISTS test_ddl_variant_glob_overlap """
    sql """ CREATE TABLE test_ddl_variant_glob_overlap (
        `id` bigint NULL,
        `var` variant<
            'a*b' : string,
            properties("variant_max_subcolumns_count" = "10")
        > NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true") """
    sql """ alter table test_ddl_variant_glob_overlap modify column var variant<
                'a*b' : string,
                'a*' : string,
                properties("variant_max_subcolumns_count" = "10")
            > NULL """
    wait_for_latest_op_on_table_finish("test_ddl_variant_glob_overlap", timeout)
    def overlapShowCreate = sql """ show create table test_ddl_variant_glob_overlap """
    assertTrue(overlapShowCreate.toString().contains("'a*'"))

    sql """ DROP TABLE IF EXISTS test_ddl_variant_glob_complex_boundary """
    sql """ CREATE TABLE test_ddl_variant_glob_complex_boundary (
        `id` bigint NULL,
        `var` variant<
            'a?*' : string,
            properties("variant_max_subcolumns_count" = "10")
        > NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true") """
    test {
        sql """ alter table test_ddl_variant_glob_complex_boundary modify column var variant<
                    'a?*' : string,
                    'ab*' : string,
                    properties("variant_max_subcolumns_count" = "10")
                > NULL """
        exception("Can not add variant schema template ab* because existing pattern a?* can shadow it")
    }
}
