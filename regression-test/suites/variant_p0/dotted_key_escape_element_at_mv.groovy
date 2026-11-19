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

import groovy.json.JsonSlurper

suite("dotted_key_escape_element_at_mv", "p0") {
    sql "set sql_mode=''"
    sql "set group_commit='off_mode'"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set batch_size=1024"
    sql "set default_variant_enable_doc_mode=false"
    sql "set default_variant_enable_nested_group=false"
    sql "set default_variant_max_subcolumns_count=0"
    sql "set default_variant_enable_typed_paths_to_sparse=false"
    sql "set default_variant_max_sparse_column_statistics_size=10000"
    sql "set enable_unique_key_partial_update=false"
    sql "set enable_variant_flatten_nested=false"
    sql "set enable_insert_strict=true"
    sql "set disable_nereids_rules=''"
    sql "set enable_inverted_index_query=true"
    sql "set enable_two_phase_read_opt=false"

    sql "set enable_prune_nested_column=true"
    sql "set enable_materialized_view_rewrite=true"
    sql "DROP TABLE IF EXISTS dotted_key_escape_element_at_mv"
    sql """CREATE TABLE dotted_key_escape_element_at_mv(k INT, v VARIANT<PROPERTIES(
        "variant_enable_nested_group"="false",
        "variant_max_subcolumns_count"="2", "variant_sparse_hash_shard_count"="3")>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO dotted_key_escape_element_at_mv VALUES
        (1,CAST(CAST('{"obj":{"a.b":1,"a":{"b":2},"a%2Eb":3}}' AS JSON) AS VARIANT))"""
    def db = context.config.getDbNameByFile(context.file)
    create_sync_mv(db, 'dotted_key_escape_element_at_mv', 'dotted_escape_obj_mv',
            "SELECT k AS kk, element_at(v,'obj') AS sub FROM dotted_key_escape_element_at_mv")
    sql """INSERT INTO dotted_key_escape_element_at_mv VALUES
        (2,CAST(CAST('{"obj":{"a.b":11,"a":{"b":12},"a%2Eb":13}}' AS JSON) AS VARIANT))"""
    def failures = []
    def expectedSub = [1: ['a.b': 1, a: [b: 2], 'a%2Eb': 3],
                       2: ['a.b': 11, a: [b: 12], 'a%2Eb': 13]]
    def check = { label, rows ->
        logger.info("ElementAt ${label}: ${rows}")
        if (rows.size() != 2) failures.add("${label}: missing rows ${rows}")
        rows.each { row ->
            def expected = expectedSub[row[0].toInteger()]
            if (row[1] == null || new JsonSlurper().parseText(row[1]) != expected ||
                    row[2]?.toString() != expected['a.b'].toString() ||
                    row[3]?.toString() != expected.a.b.toString()) {
                failures.add("${label}: ${row}, expected ${expected}")
            }
        }
    }
    for (def rules : ['', 'VARIANT_SUB_PATH_PRUNING']) {
        sql "set disable_nereids_rules='${rules}'"
        def projection = """SELECT k, element_at(v,'obj') AS sub,
            v['obj']['a.b'], v['obj']['a']['b'] FROM dotted_key_escape_element_at_mv"""
        sql "set enable_materialized_view_rewrite=false"
        check("base rules=${rules}", sql("${projection} INDEX dotted_key_escape_element_at_mv ORDER BY k"))
        sql "set enable_materialized_view_rewrite=true"
        def plan = sql("EXPLAIN VERBOSE ${projection} ORDER BY k").collect { it[0] }.join('\n')
        logger.info("ElementAt plan rules=${rules}:\n${plan}")
        if (rules == '' && !plan.contains('dotted_key_escape_element_at_mv(dotted_escape_obj_mv)')) failures.add("MV not chosen rules=${rules}")
        check("rewrite rules=${rules}", sql("${projection} ORDER BY k"))
        sql "set enable_materialized_view_rewrite=false"
        def mvQuery = """SELECT kk,sub,sub['a.b'],sub['a']['b']
            FROM dotted_key_escape_element_at_mv INDEX dotted_escape_obj_mv ORDER BY kk"""
        explain {
            sql mvQuery
            contains("dotted_key_escape_element_at_mv(dotted_escape_obj_mv)")
        }
        check("physical MV rules=${rules}", sql(mvQuery))
    }
    assertTrue(failures.isEmpty(), failures.join('\n'))
}
