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

// TODO(lihangyu): need to be fixed
// // this test is used to test the type conflict of nested array
// suite("variant_nested_type_conflict", "p0"){
// 
//     try {
// 
//         def table_name = "var_nested_type_conflict"
//         sql "DROP TABLE IF EXISTS ${table_name}"
//         sql """set describe_extend_variant_column = true"""
// 
//         sql """ set disable_variant_flatten_nested = false """
//         sql """
//                 CREATE TABLE IF NOT EXISTS ${table_name} (
//                     k bigint,
//                     v variant
//                 )
//                 DUPLICATE KEY(`k`)
//                 DISTRIBUTED BY HASH(k) BUCKETS 1 -- 1 bucket make really compaction in conflict case
//                 properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true");
//             """
//         def sql_select_batch = {
//             qt_sql_0 """select * from ${table_name} order by k"""
// 
//             qt_sql_1 """select v['nested']['a'] from ${table_name} order by k"""
//             qt_sql_2 """select v['nested']['b'] from ${table_name} order by k"""
//             qt_sql_3 """select v['nested']['c'] from ${table_name} order by k"""
// 
//             qt_sql_4 """select v['nested'] from ${table_name} order by k"""
//         }
// 
//         def sql_test_cast_to_array = {
//             // test cast to array<int> 
//             qt_sql_8 """select cast(v['nested']['a'] as array<int>), size(cast(v['nested']['a'] as array<int>)) from ${table_name} order by k"""
//             qt_sql_9 """select cast(v['nested']['b'] as array<int>), size(cast(v['nested']['b'] as array<int>)) from ${table_name} order by k"""
//             qt_sql_10 """select cast(v['nested']['c'] as array<int>), size(cast(v['nested']['c'] as array<int>)) from ${table_name} order by k"""
// 
//             // test cast to array<string> 
//             qt_sql_11 """select cast(v['nested']['a'] as array<string>), size(cast(v['nested']['a'] as array<string>)) from ${table_name} order by k"""
//             qt_sql_12 """select cast(v['nested']['b'] as array<string>), size(cast(v['nested']['b'] as array<string>)) from ${table_name} order by k"""
//             qt_sql_13 """select cast(v['nested']['c'] as array<string>), size(cast(v['nested']['c'] as array<string>)) from ${table_name} order by k"""
// 
//             // test cast to array<double> 
//             qt_sql_14 """select cast(v['nested']['a'] as array<double>), size(cast(v['nested']['a'] as array<double>)) from ${table_name} order by k"""
//             qt_sql_15 """select cast(v['nested']['b'] as array<double>), size(cast(v['nested']['b'] as array<double>)) from ${table_name} order by k"""
//             qt_sql_16 """select cast(v['nested']['c'] as array<double>), size(cast(v['nested']['c'] as array<double>)) from ${table_name} order by k"""
// 
//         }
//         // insert Nested array in Nested array which is not supported
//         test {
//             sql """
//                 insert into ${table_name} values (1, '{"nested": [{"a": [1,2,3]}]}');
//                 """
//             exception "Nesting of array in Nested array within variant subcolumns is currently not supported."
//         }
//         // insert batch different structure in same path
//         test {
//             sql """
//                 insert into ${table_name} values (3, '{"nested": [{"a": 2.5, "b": "123.1"}]}'),  (4, '{"nested": {"a": 2.5, "b": "123.1"}}');
//                 """
//             exception "Ambiguous paths"
//         }
//         /// insert a array of object for a, b, c 
//         // insert type conflict in multiple rows
//         sql """
//             insert into ${table_name} values (1, '{"nested": [{"a": 1, "c": 1.1}, {"b": "1"}]}'); 
//             """
// 
//         // for cloud we should select first and then desc for syncing rowset to get latest schema
//         sql """
//             select * from ${table_name} order by k limit 1;
//             """
//         qt_sql_desc_1 """
//             desc ${table_name};
//             """
//         // now select for a, b, c
//         sql_select_batch()
//         sql_test_cast_to_array()
//         /// insert a, b type changed to double 
//         sql """
//             insert into ${table_name} values (2, '{"nested": [{"a": 2.5, "b": 123.1}]}');
//             """
//         // for cloud we should select first and then desc for syncing rowset to get latest schema
//         sql """
//             select * from ${table_name} order by k limit 1;
//             """
//         qt_sql_desc_2 """
//             desc ${table_name};
//             """
//         // now select for a, b, c
//         sql_select_batch()
//         sql_test_cast_to_array()
// 
//         // trigger and wait compaction
//         trigger_and_wait_compaction("${table_name}", "full")
// 
//         // now select for a, b, c
//         sql_select_batch()
//         sql_test_cast_to_array()
// 
//         sql """ truncate table ${table_name} """
// 
// 
//         // insert type conflict in one row
//         sql """
//             insert into ${table_name} values (1, '{"nested": [{"a": 1, "b": 1.1}, {"a": "1", "b": "1", "c": "1"}]}');
//             """
//         // for cloud we should select first and then desc for syncing rowset to get latest schema
//         sql """
//             select * from ${table_name} order by k limit 1;
//             """
//         qt_sql_desc_4 """
//             desc ${table_name};
//             """
//         // now select for a, b, c
//         sql_select_batch()
//         sql_test_cast_to_array()
// 
//         // insert c type changed to double
//         sql """
//             insert into ${table_name} values (2, '{"nested": [{"a": 1, "c": 1.1}]}');
//             """
//         // for cloud we should select first and then desc for syncing rowset to get latest schema
//         sql """
//             select * from ${table_name} order by k limit 1;
//             """
//         qt_sql_desc_5 """
//             desc ${table_name};
//             """
//         // now select for a, b, c
//         sql_select_batch()
//         sql_test_cast_to_array()
// 
//         // trigger and wait compaction
//         trigger_and_wait_compaction("${table_name}", "full")
// 
//         // now select for a, b, c
//         sql_select_batch()
//         sql_test_cast_to_array()
// 
//     } finally {
//     }
// 
// }
// 