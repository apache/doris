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
 
suite("update_test_index_load", "nonConcurrent,p0") {

    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            set 'memtable_on_sink_node', 'true'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

     
    def create_table_load_data = {create_table_name, format->
        sql """ set disable_inverted_index_v1_for_variant = false """
        sql "DROP TABLE IF EXISTS ${create_table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${create_table_name} (
                k bigint,
                v variant NOT NULL,
                INDEX idx(v) USING INVERTED PROPERTIES("parser"="standard")
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 10
            properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "bloom_filter_columns" = "v",
            "inverted_index_storage_format" = ${format},
             "variant_max_subcolumns_count" = "9999"
            );
        """

        for (int i = 0; i < 3; i++) {
           load_json_data.call(create_table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
        }
        try {
            GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
            sql "set enable_common_expr_pushdown = true"
            sql """set enable_match_without_inverted_index = false""" 
            sql """ set inverted_index_skip_threshold = 0 """
            sql """ set enable_inverted_index_query = true """ 
            qt_sql """ select count() from ${create_table_name} """
            qt_sql """ select count() from ${create_table_name} where cast (v['repo']['name'] as string) match 'github'"""
            qt_sql """ select count() from ${create_table_name} where cast (v['actor']['id'] as int) > 1575592 """
            qt_sql """ select count() from ${create_table_name} where cast (v['actor']['id'] as int) > 1575592 and  cast (v['repo']['name'] as string) match 'github'"""
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
        }
        

        try {
            GetDebugPoint().enableDebugPointForAllBEs("bloom_filter_must_filter_data")
            sql """ set enable_inverted_index_query = false """ 
            // number
            qt_sql1 """ select count() from ${create_table_name} where cast(v['repo']['id'] as int) in (0, 1, 2, 3, 4, 5); """

            // string
            qt_sql2 """ select count() from ${create_table_name} where cast(v['repo']['name'] as text) = "xxxx"; """
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("bloom_filter_must_filter_data")
        }
    }

    create_table_load_data.call("test_update_index_sc_v1", "V1")
    create_table_load_data.call("test_update_index_sc_v2", "V2")
    create_table_load_data.call("test_update_index_sc2_v1", "V1")
    create_table_load_data.call("test_update_index_sc2_v2", "V2")
    create_table_load_data.call("test_update_index_compact_v1", "V1")
    create_table_load_data.call("test_update_index_compact_v2", "V2")
    create_table_load_data.call("test_update_index_compact2_v1", "V1")
    create_table_load_data.call("test_update_index_compact2_v2", "V2")
}
 