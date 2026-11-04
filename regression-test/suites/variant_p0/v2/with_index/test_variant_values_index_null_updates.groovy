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

suite("test_variant_values_index_null_updates", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    for (boolean nativeMerge : [true, false]) {
        for (boolean vertical : [true, false]) {
            setBeConfigTemporary([
                enable_vertical_compaction: vertical,
                enable_ordered_data_compaction: false,
                inverted_index_compaction_enable: nativeMerge
            ]) {
                sql "DROP TABLE IF EXISTS vvi_null_updates"
                sql """CREATE TABLE vvi_null_updates (id INT,v VARIANT,
                    INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"))
                    UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true",
                               "inverted_index_storage_format"="SNII","disable_auto_compaction"="true")"""
                sql """INSERT INTO vvi_null_updates VALUES (2,parse_to_variant('{"s":"retained"}'))"""
                def queries = [
                    "SELECT id,CAST(v AS STRING),v IS NULL,v MATCH_ANY 'alpha',v MATCH_ANY 'omega' FROM vvi_null_updates ORDER BY id",
                    "SELECT id FROM vvi_null_updates WHERE v MATCH_ANY 'alpha' ORDER BY id",
                    "SELECT id FROM vvi_null_updates WHERE v MATCH_ANY 'omega' ORDER BY id",
                    "SELECT id FROM vvi_null_updates WHERE NOT(v MATCH_ANY 'alpha omega') ORDER BY id",
                    "SELECT id FROM vvi_null_updates WHERE v MATCH_ANY 'retained' ORDER BY id"
                ]
                ['''parse_to_variant('{"s":"alpha"}')''', "NULL", "parse_to_variant('{}')",
                 "parse_to_variant('null')", '''parse_to_variant('{"s":"omega"}')'''].eachWithIndex { value, stage ->
                    sql "INSERT INTO vvi_null_updates VALUES (1,${value})"
                    sql "SYNC"
                    def before = []
                    for (String phase : ["before", "after"]) {
                        if (phase == "after") {
                            trigger_and_wait_compaction("vvi_null_updates", "full", 1800)
                        }
                        queries.eachWithIndex { query, i ->
                            sql "SET enable_inverted_index_query=true"
                            sql "SET enable_match_without_inverted_index=${i == 0}"
                            def indexed = sql(query)
                            sql "SET enable_inverted_index_query=false"
                            sql "SET enable_match_without_inverted_index=true"
                            assertEquals(indexed, sql(query))
                            if (phase == "before") {
                                before.add(indexed)
                                if (nativeMerge && vertical) {
                                    quickTest("stage_${stage}_${i}", query, true)
                                }
                            } else {
                                assertEquals(before[i], indexed)
                            }
                        }
                    }
                }
            }
        }
    }
}
