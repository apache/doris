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

import org.apache.doris.regression.action.ProfileAction

suite("dotted_key_escape_index_profile", "p0") {
    sql "set default_variant_enable_doc_mode=false"
    sql "drop table if exists dotted_key_escape_index_profile"
    sql """create table dotted_key_escape_index_profile(id int,v variant<properties(
        "variant_enable_nested_group"="false",
        "variant_max_subcolumns_count"="0")>,index idx_v(v) using inverted)
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "inverted_index_storage_format"="V2")"""
    // Keep matching and nonmatching values in one segment so zone maps cannot
    // eliminate the filler segment before the inverted index sees its rows.
    def fillers = (2..65).collect { id ->
        "(${id},parse_to_variant('{\"a.b\":-1,\"a\":{\"b\":-1},\"a%2Eb\":-1}'))"
    }.join(",")
    sql """insert into dotted_key_escape_index_profile values
        (1,parse_to_variant('{"a.b":101,"a":{"b":201},"a%2Eb":301}')),${fillers}"""
    sql "set inverted_index_skip_threshold=0"
    sql "set enable_condition_cache=false"
    sql "set enable_profile=true"
    sql "set profile_level=2"
    try {
        [["v['a.b']",101],["v['a']['b']",201],["v['a%2Eb']",301]].eachWithIndex { path, index ->
            def query = "select /* dotted_key_numeric_index_${index} */ id from dotted_key_escape_index_profile where cast(${path[0]} as bigint)=${path[1]}"
            sql "set enable_inverted_index_query=true"
            order_qt_numeric_index_on query
            def profile = new ProfileAction(context).getProfileBySql(
                    "dotted_key_numeric_index_${index}", ["RowsInvertedIndexFiltered"])
            def counts = (profile =~ /RowsInvertedIndexFiltered:\s*(\d+)/).collect { it[1] as long }
            logger.info("Escaped numeric index ${path[0]} filtered rows: ${counts}")
            assertTrue(counts.any { it > 0 }, "The ${path[0]} predicate did not use its inverted index: ${profile}")
            sql "set enable_inverted_index_query=false"
            order_qt_numeric_index_off query
        }
    } finally {
        sql "set enable_profile=false"
        sql "set enable_inverted_index_query=true"
    }
}
