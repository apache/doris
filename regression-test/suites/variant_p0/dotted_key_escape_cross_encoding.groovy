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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

suite("dotted_key_escape_cross_encoding", "p0") {
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

    def documents = [
        ['a': ['b': 7], 'a%2Eb': 3, '50%': 4, '': 5, nested: [x: 6], arr: [1, 2]],
        ['a.b': 1, 'a%2Eb': 3, '50%': 4, '': 5, nested: [x: 6], arr: [1, 2]],
        '123', 'true', 'null', '{}']
    for (def format : ['V2', 'V3']) {
        for (def docMode : [false, true]) {
            def layout = docMode
                    ? '"variant_enable_doc_mode"="true", "variant_doc_hash_shard_count"="3", "variant_doc_materialization_min_rows"="1"'
                    : '"variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="3"'

            for (def sourceEscape : [false, true]) {
                def source = "dotted_cross_source_${format}_${docMode}_${sourceEscape}"
                def target = "dotted_cross_target_${format}_${docMode}_${sourceEscape}"
                [[source, sourceEscape], [target, !sourceEscape]].each { pair ->
                    sql "DROP TABLE IF EXISTS ${pair[0]}"
                    sql """CREATE TABLE ${pair[0]} (id INT, v VARIANT<PROPERTIES(
                        ${layout},
                        "variant_enable_nested_group"="false")>)
                        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES("variant_escape_dots_in_keys"="${pair[1]}", "replication_num"="1", "disable_auto_compaction"="true",
                                   "storage_format"="${format}")"""
                }
                documents.eachWithIndex { value, id ->
                    sql "INSERT INTO ${source} VALUES (${id},CAST(CAST('${JsonOutput.toJson(value)}' AS JSON) AS VARIANT))"
                }
                sql "INSERT INTO ${target} SELECT id,v FROM ${source}"
                def rows = sql "SELECT id,CAST(v AS JSON) FROM ${target} ORDER BY id"
                logger.info("Cross encoding ${format} doc=${docMode} sourceEscape=${sourceEscape}: ${rows}")
                assertEquals(documents.size(), rows.size())
                rows.each { row ->
                    def expected = documents[row[0].toInteger()]
                    if (row[0].toInteger() == 1) {
                        expected = ['a': ['b': 1], 'a%2Eb': 3, '50%': 4, '': 5,
                                    nested: [x: 6], arr: [1, 2]]
                    }
                    assertEquals(expected, new JsonSlurper().parseText(row[1]))
                    assertFalse(row[1].contains('50%25'))
                    assertFalse(row[1].contains('a%252Eb'))
                }
            }
        }
    }
}
