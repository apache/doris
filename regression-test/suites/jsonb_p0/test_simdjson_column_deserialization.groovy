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

suite("test_simdjson_column_deserialization", "p0") {
    sql "DROP TABLE IF EXISTS test_simdjson_column_deserialization"
    sql """CREATE TABLE test_simdjson_column_deserialization (
        id INT, small TINYINT, flag BOOLEAN, amount DECIMAL(20,4), payload JSONB,
        items ARRAY<STRUCT<x:INT,y:STRING>>, attrs MAP<STRING,ARRAY<INT>>,
        first_text STRING, second_text STRING
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    streamLoad {
        table "test_simdjson_column_deserialization"
        set "format", "json"
        set "read_json_by_line", "true"
        set "strict_mode", "true"
        set "max_filter_ratio", "0.5"
        file "simdjson_column_deserialization.json"
        time 10000
    }
    sql "sync"
    order_qt_by_name """SELECT id,small,flag,amount,CAST(payload AS STRING),items,attrs,
                              first_text,second_text
                       FROM test_simdjson_column_deserialization"""
    sql "TRUNCATE TABLE test_simdjson_column_deserialization"
    streamLoad {
        table "test_simdjson_column_deserialization"
        set "format", "json"
        set "read_json_by_line", "true"
        set "strict_mode", "true"
        set "max_filter_ratio", "0.5"
        set "jsonpaths", '["$.id","$.small","$.flag","$.amount","$.payload","$.items","$.attrs","$.first_text","$.first_text"]'
        file "simdjson_column_deserialization.json"
        time 10000
    }
    sql "sync"
    order_qt_by_path """SELECT id,small,flag,amount,CAST(payload AS STRING),items,attrs,
                              first_text,second_text
                       FROM test_simdjson_column_deserialization"""
}
