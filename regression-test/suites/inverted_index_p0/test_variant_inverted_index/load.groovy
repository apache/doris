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

suite("load") {

    sql """ DROP TABLE IF EXISTS test_variant_inverted_index; """

    sql """ CREATE TABLE IF NOT EXISTS test_variant_inverted_index_null
    (
        col_int_undef_signed_not_null int not null,
        col_variant           variant null,
        col_variant_parser_eng       variant null,
        col_variant_parser_uni         variant null,
        INDEX                      col_variant_idx (`col_variant`) USING INVERTED,
        INDEX                      col_variant_parser_eng_idx (`col_variant_parser_eng`) USING INVERTED PROPERTIES("parser" = "english"),
        INDEX                      col_variant_parser_uni_idx (`col_variant_parser_uni`) USING INVERTED PROPERTIES("parser" = "unicode"),
    ) engine=olap
    UNIQUE KEY(col_int_undef_signed_not_null)
    distributed by hash(col_int_undef_signed_not_null)
    properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1"); """



    sql """ DROP TABLE IF EXISTS test_variant_inverted_index_not_null; """
    sql """ CREATE TABLE IF NOT EXISTS test_variant_inverted_index_not_null
    (
        col_int_undef_signed_not_null int not null,
        col_variant           variant not null,
        col_variant_parser_eng       variant not null,
        col_variant_parser_uni         variant not null,
        INDEX                      col_variant_idx (`col_variant`) USING INVERTED,
        INDEX                      col_variant_parser_eng_idx (`col_variant_parser_eng`) USING INVERTED PROPERTIES("parser" = "english"),
        INDEX                      col_variant_parser_uni_idx (`col_variant_parser_uni`) USING INVERTED PROPERTIES("parser" = "unicode"),
    ) UNIQUE KEY(col_int_undef_signed_not_null)
          distributed by hash(col_int_undef_signed_not_null)
          properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1"); """

    // stream_load
    def load_data = {table_name, strip_flag, read_flag, format_flag, exprs, json_root, json_paths,
                             where_expr, fuzzy_flag, column_sep, file_name ->
        streamLoad {
            table table_name

            // set http request header params
            set 'read_json_by_line', read_flag
            set 'strip_outer_array', strip_flag
            set 'format', format_flag
            set 'columns', exprs
            set 'jsonpaths', json_paths
            set 'json_root', json_root
            set 'where', where_expr
            set 'fuzzy_parse', fuzzy_flag
            set 'column_separator', column_sep
            set 'max_filter_ratio', '0.6'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows + json.NumberFilteredRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    // load data :table_name, strip_flag, read_flag, format_flag, exprs, json_root, json_paths,
    //             where_expr, fuzzy_flag, column_sep, file_name
    def file="test_variant_inverted_index.csv"
    load_data.call("test_variant_inverted_index_null", "false", "false", "csv", null, null, null, null, null, "\t", file)
    load_data.call("test_variant_inverted_index_not_null", "false", "false", "csv", null, null, null, null, null, "\t", file)
}
