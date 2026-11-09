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

suite("test_variant_light_max_subcolumns_two_columns", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS variant_light_budget_two"
    sql """CREATE TABLE variant_light_budget_two (id INT,
        v1 VARIANT<PROPERTIES('variant_max_subcolumns_count'='2')>,
        v2 VARIANT<PROPERTIES('variant_max_subcolumns_count'='5')>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
    sql "DROP TABLE IF EXISTS variant_light_budget_two_control"
    sql """CREATE TABLE variant_light_budget_two_control (id INT,
        v1 VARIANT<PROPERTIES('variant_max_subcolumns_count'='0')>,
        v2 VARIANT<PROPERTIES('variant_max_subcolumns_count'='5')>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
    def rows = (1..3).collect { id ->
        def a = (0..<12).collect { p -> "\"a${p}\":${id * 10 + p}" }.join(',')
        def b = (0..<9).collect { p -> "\"b${p}\":${id * 20 + p}" }.join(',')
        "(${id},parse_to_variant('{${a}}'),parse_to_variant('{${b}}'))"
    }
    sql "INSERT INTO variant_light_budget_two VALUES ${rows.take(2).join(', ')}"
    sql "ALTER TABLE variant_light_budget_two MODIFY COLUMN v1 VARIANT<PROPERTIES('variant_max_subcolumns_count'='0')>"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_budget_two' ORDER BY CreateTime DESC LIMIT 1"
        time 30
    }
    sql "INSERT INTO variant_light_budget_two VALUES ${rows[2]}"
    order_qt_mixed "SELECT id, v1, v2 FROM variant_light_budget_two"
    sql "INSERT INTO variant_light_budget_two_control VALUES ${rows.join(', ')}"
    trigger_and_wait_compaction('variant_light_budget_two', 'full')
    order_qt_whole_path_invariant """SELECT id FROM variant_light_budget_two
        WHERE NOT (CAST(v1['a0'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a0')) OR NOT (CAST(v1['a1'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a1')) OR NOT (CAST(v1['a2'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a2')) OR NOT (CAST(v1['a3'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a3')) OR NOT (CAST(v1['a4'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a4')) OR NOT (CAST(v1['a5'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a5')) OR NOT (CAST(v1['a6'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a6')) OR NOT (CAST(v1['a7'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a7')) OR NOT (CAST(v1['a8'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a8')) OR NOT (CAST(v1['a9'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a9')) OR NOT (CAST(v1['a10'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a10')) OR NOT (CAST(v1['a11'] AS STRING) <=> get_json_string(CAST(v1 AS STRING), '\$.a11')) OR NOT (CAST(v2['b0'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b0')) OR NOT (CAST(v2['b1'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b1')) OR NOT (CAST(v2['b2'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b2')) OR NOT (CAST(v2['b3'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b3')) OR NOT (CAST(v2['b4'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b4')) OR NOT (CAST(v2['b5'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b5')) OR NOT (CAST(v2['b6'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b6')) OR NOT (CAST(v2['b7'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b7')) OR NOT (CAST(v2['b8'] AS STRING) <=> get_json_string(CAST(v2 AS STRING), '\$.b8'))"""
    order_qt_compacted "SELECT id, v1, v2, v1['a11'], v2['b8'] FROM variant_light_budget_two"
    order_qt_control "SELECT id, v1, v2, v1['a11'], v2['b8'] FROM variant_light_budget_two_control"
    order_qt_difference """SELECT 'altered_only', delta.* FROM (
        SELECT id, CAST(v1 AS STRING), CAST(v2 AS STRING) FROM variant_light_budget_two
        EXCEPT SELECT id, CAST(v1 AS STRING), CAST(v2 AS STRING) FROM variant_light_budget_two_control) delta
        UNION ALL SELECT 'control_only', delta.* FROM (
        SELECT id, CAST(v1 AS STRING), CAST(v2 AS STRING) FROM variant_light_budget_two_control
        EXCEPT SELECT id, CAST(v1 AS STRING), CAST(v2 AS STRING) FROM variant_light_budget_two) delta"""
}
