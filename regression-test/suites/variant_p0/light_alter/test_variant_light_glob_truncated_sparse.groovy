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

suite("test_variant_light_glob_truncated_sparse", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS variant_light_glob_truncated"
    sql """CREATE TABLE variant_light_glob_truncated (id INT,
        v VARIANT<PROPERTIES('variant_max_subcolumns_count'='1',
            'variant_max_sparse_column_statistics_size'='1')>, w VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
    sql "DROP TABLE IF EXISTS variant_light_glob_truncated_control"
    sql """CREATE TABLE variant_light_glob_truncated_control (id INT,
        v VARIANT<'z*':INT, PROPERTIES('variant_max_subcolumns_count'='1',
            'variant_max_sparse_column_statistics_size'='1')>, w VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
    // z_tail sorts beyond the capped sparse statistics; hot wins the materialization budget.
    def rows = [
        '''(0,parse_to_variant('{"hot":1,"a":2,"b":3,"z_tail":"7"}'),parse_to_variant('{"keep":9}'))''',
        '''(1,parse_to_variant('{"hot":1}'),parse_to_variant('{"keep":8}'))''',
        '''(2,parse_to_variant('{"hot":1}'),parse_to_variant('{"keep":7}'))''',
        '''(3,parse_to_variant('{"hot":1}'),parse_to_variant('{"keep":6}'))''',
        '''(4,parse_to_variant('{"z_tail":8,"z_other":"9"}'),parse_to_variant('{"keep":5}'))'''
    ]
    sql "INSERT INTO variant_light_glob_truncated VALUES ${rows.take(4).join(', ')}"
    sql """ALTER TABLE variant_light_glob_truncated MODIFY COLUMN v VARIANT<'z*':INT,
        PROPERTIES('variant_max_subcolumns_count'='1', 'variant_max_sparse_column_statistics_size'='1')>"""
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_glob_truncated' ORDER BY CreateTime DESC LIMIT 1"
        time 30
    }
    sql "INSERT INTO variant_light_glob_truncated VALUES ${rows[4]}"
    sql "INSERT INTO variant_light_glob_truncated_control VALUES ${rows.join(', ')}"
    trigger_and_wait_compaction('variant_light_glob_truncated', 'full')
    order_qt_whole_path_invariant """SELECT id FROM variant_light_glob_truncated
        WHERE NOT (CAST(v['z_tail'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.z_tail')) OR NOT (CAST(v['z_other'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.z_other')) OR NOT (CAST(w['keep'] AS STRING) <=> get_json_string(CAST(w AS STRING), '\$.keep'))"""
    order_qt_compacted "SELECT id, v, w, v['z_tail'], v['z_other'] FROM variant_light_glob_truncated"
    order_qt_control "SELECT id, v, w, v['z_tail'], v['z_other'] FROM variant_light_glob_truncated_control"
    order_qt_difference """SELECT 'altered_only', delta.* FROM (
        SELECT id, CAST(v AS STRING), CAST(w AS STRING) FROM variant_light_glob_truncated
        EXCEPT SELECT id, CAST(v AS STRING), CAST(w AS STRING) FROM variant_light_glob_truncated_control) delta
        UNION ALL SELECT 'control_only', delta.* FROM (
        SELECT id, CAST(v AS STRING), CAST(w AS STRING) FROM variant_light_glob_truncated_control
        EXCEPT SELECT id, CAST(v AS STRING), CAST(w AS STRING) FROM variant_light_glob_truncated) delta"""
}
