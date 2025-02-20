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

suite("update_test_index_load", "p0") {

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


    def create_table_load_data = {create_table_name->
        sql "DROP TABLE IF EXISTS ${create_table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${create_table_name} (
                k bigint,
                v variant NOT NULL,
                INDEX idx(v) USING INVERTED PROPERTIES("parser"="standard")
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 6
            properties("replication_num" = "1", "disable_auto_compaction" = "true", "variant_max_subcolumns_count" = "0");
        """

        for (int i = 0; i < 10; i++) {
           load_json_data.call(create_table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
        }
        sql """set enable_match_without_inverted_index = false""" 
        sql """ set inverted_index_skip_threshold = 0 """
        qt_sql """ select count() from ${create_table_name} """
        qt_sql """ select count() from ${create_table_name} where cast (v['repo']['name'] as string) match 'github'"""
        qt_sql """ select count() from ${create_table_name} where cast (v['actor']['id'] as int) > 1575592 """
        qt_sql """ select count() from ${create_table_name} where cast (v['actor']['id'] as int) > 1575592 and  cast (v['repo']['name'] as string) match 'github'"""
    }

    create_table_load_data.call("test_update_index_sc")
    create_table_load_data.call("test_update_index_sc2")
    create_table_load_data.call("test_update_index_compact")
    create_table_load_data.call("test_update_index_compact2")
}
