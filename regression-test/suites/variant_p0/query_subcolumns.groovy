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

suite("regression_test_query_subcolumns", "nonConcurrent"){

    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }
    sql """DROP TABLE IF EXISTS query_subcolumns"""

    // query must use inverted index for match operator
    sql """ set enable_common_expr_pushdown = true; """
    sql "set enable_match_without_inverted_index = false"
    sql "set default_variant_enable_doc_mode = false"
    for (int i = 0; i < 5; i++) {
        int max_subcolumns_count = Math.floor(Math.random() * 10)
        // int max_subcolumns_count =1 
        def var = """variant<properties(\"variant_max_subcolumns_count\" = \"${max_subcolumns_count}\")>,
                    INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english") """
        if (max_subcolumns_count % 2 == 0) {
            var = """variant <'d' : int, 'b' : int, 'e' : string, properties(\"variant_max_subcolumns_count\" = \"${max_subcolumns_count}\")>,
                    INDEX idx_v_e (v) USING INVERTED PROPERTIES("field_pattern" = "e", "parser" = "english", "support_phrase" = "true"),
                    INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english") """
        }
        sql "DROP TABLE IF EXISTS query_subcolumns"
        sql """
            CREATE TABLE IF NOT EXISTS query_subcolumns (
                    k bigint,
                    v ${var}
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
        """
        sql """insert into query_subcolumns values (1, '{"a" : 1, "b" : "2"}')"""
        // legacy V2 write
        sql """insert into query_subcolumns values (2, '{"a" : 1, "b" : "2", "c" : 3}')"""
        // switch to V2.1
        sql """insert into query_subcolumns values (3, '{"a" : 1, "b" : "2", "c" : 3, "d" : 4}')"""
        // keep V2.1
        sql """insert into query_subcolumns values (4, '{"a" : 1}')"""
        // switch back to V2
        sql """insert into query_subcolumns values (5, '{"e" : "hello, world"}')"""
        // and again V2.1
        sql """insert into query_subcolumns values (6, '{"f" : "make it work"}')"""

        qt_sql "select v['a'] from query_subcolumns where cast(v['a'] as int) is not null order by k"
        qt_sql "select v['b'] from query_subcolumns where cast(v['b'] as int) is not null order by k"
        qt_sql "select v['c'] from query_subcolumns where cast(v['c'] as int) is not null order by k"
        qt_sql "select v['d'] from query_subcolumns where cast(v['d'] as int) is not null order by k"
        qt_sql "select v['e'] from query_subcolumns where v['e'] like 'hello%' order by k"
        if (max_subcolumns_count >= 6) {
            sql "select v['f'] from query_subcolumns where v['f'] match 'make' order by k"
        }

        // if (max_subcolumns_count % 2) {
        //     // sql """alter table query_subcolumns set ("storage_format" = "V3")"""
        // } else {
        //     // sql """alter table query_subcolumns set ("storage_format" = "V2")"""
        // }

        // triger compaction
        trigger_and_wait_compaction("query_subcolumns", "full", 1800)        

        qt_sql "select v['a'] from query_subcolumns where cast(v['a'] as int) is not null order by k"
        qt_sql "select v['b'] from query_subcolumns where cast(v['b'] as int) is not null order by k"
        qt_sql "select v['c'] from query_subcolumns where cast(v['c'] as int) is not null order by k"
        qt_sql "select v['d'] from query_subcolumns where cast(v['d'] as int) is not null order by k"
        qt_sql "select v['e'] from query_subcolumns where v['e'] like 'hello%' order by k"
        if (max_subcolumns_count >= 6) {
            sql "select v['f'] from query_subcolumns where v['f'] match 'make' order by k"
        }
    }
}