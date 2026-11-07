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

suite("test_variant_values_index_promotion_contract", "p0,nonConcurrent") {
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=10"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    for (boolean nativeMerge : [true,false]) {
        setBeConfigTemporary([inverted_index_compaction_enable:nativeMerge,enable_ordered_data_compaction:false]) {
            sql "DROP TABLE IF EXISTS vvi_promotion_contract"
            sql """CREATE TABLE vvi_promotion_contract(id INT,v VARIANT,
                INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"))
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
            sql """INSERT INTO vvi_promotion_contract VALUES(1,parse_to_variant('{"n":true}'))"""
            sql """INSERT INTO vvi_promotion_contract VALUES(2,parse_to_variant('{"n":1}'))"""
            for (String phase : ["before","after"]) {
                if (phase == "after") { trigger_and_wait_compaction("vvi_promotion_contract", "full", 1800) }
                quickTest("${nativeMerge}_${phase}_stored", "SELECT id,v,CAST(v['n'] AS STRING) FROM vvi_promotion_contract ORDER BY id", true)
                // Accepted contract: native merge retains 'true' even when stored n promotes to 1.
                // Rebuild instead indexes the promoted stored value. Keep each term independent.
                for (boolean indexed : [true,false]) {
                    sql "SET enable_inverted_index_query=${indexed}"
                    sql "SET enable_match_without_inverted_index=${!indexed}"
                    quickTest("${nativeMerge}_${phase}_${indexed}_true", "SELECT id FROM vvi_promotion_contract WHERE v MATCH_ANY 'true' ORDER BY id", true)
                    quickTest("${nativeMerge}_${phase}_${indexed}_one", "SELECT id FROM vvi_promotion_contract WHERE v MATCH_ANY '1' ORDER BY id", true)
                }
            }
        }
    }
}
