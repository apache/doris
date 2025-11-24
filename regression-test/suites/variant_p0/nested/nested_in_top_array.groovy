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

// this test is used to test the nested in top array
suite("nested_in_top_array", "p0"){

    try {

        // create a table with nested in top array
        def table_name = "var_nested_in_top_array"
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """set describe_extend_variant_column = true"""

        sql """ set default_variant_max_subcolumns_count = 0"""
        // set disable_variant_flatten_nested = false to enable variant flatten nested
        sql """ set enable_variant_flatten_nested = true """
        sql """
                    CREATE TABLE IF NOT EXISTS ${table_name} (
                        k bigint,
                        v variant
                    )
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 -- 1 bucket make really compaction in conflict case
                    properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true");
                """
        sql """ insert into ${table_name} values (1, '[{"a": 1, "c": 1.1}, {"b": "1"}]'); """ 
        
       def sql_select_batch = { tn ->
            qt_sql_0 """select * from ${tn} order by k"""

            qt_sql_1 """select v['a'] from ${tn} order by k"""
            qt_sql_2 """select v['b'] from ${tn} order by k"""
            qt_sql_3 """select v['c'] from ${tn} order by k"""

            qt_sql_4 """select v from ${tn} order by k"""
        }

        def sql_test_cast_to_array = { tn ->
            // test cast to array<int> 
            qt_sql_8 """select cast(v['a'] as array<int>), size(cast(v['a'] as array<int>)) from ${tn} order by k"""
            qt_sql_9 """select cast(v['b'] as array<int>), size(cast(v['b'] as array<int>)) from ${tn} order by k"""
            qt_sql_10 """select cast(v['c'] as array<int>), size(cast(v['c'] as array<int>)) from ${tn} order by k"""

            // test cast to array<string> 
            qt_sql_11 """select cast(v['a'] as array<string>), size(cast(v['a'] as array<string>)) from ${tn} order by k"""
            qt_sql_12 """select cast(v['b'] as array<string>), size(cast(v['b'] as array<string>)) from ${tn} order by k"""
            qt_sql_13 """select cast(v['c'] as array<string>), size(cast(v['c'] as array<string>)) from ${tn} order by k"""

            // test cast to array<double> 
            qt_sql_14 """select cast(v['a'] as array<double>), size(cast(v['a'] as array<double>)) from ${tn} order by k"""
            qt_sql_15 """select cast(v['b'] as array<double>), size(cast(v['b'] as array<double>)) from ${tn} order by k"""
            qt_sql_16 """select cast(v['c'] as array<double>), size(cast(v['c'] as array<double>)) from ${tn} order by k"""

        }

        def sql_test_cast_to_scalar = { tn ->
            qt_sql_17 """select cast(v['a'] as int), cast(v['b'] as int), cast(v['c'] as int) from ${tn} order by k"""
            qt_sql_18 """select cast(v['a'] as string), cast(v['b'] as string), cast(v['c'] as string) from ${tn} order by k"""
            qt_sql_19 """select cast(v['a'] as double), cast(v['b'] as double), cast(v['c'] as double) from ${tn} order by k"""
        }


        sql_select_batch(table_name)
        sql_test_cast_to_array(table_name)
        sql_test_cast_to_scalar(table_name)

        //  insert with type conflict for a, b
        sql """
            insert into ${table_name} values (2, '[{"a": "2.5", "b": 123.1}]');
            """
        sql_select_batch(table_name)
        sql_test_cast_to_array(table_name)
        sql_test_cast_to_scalar(table_name)

        // insert with structure conflict for a
        sql """
            insert into ${table_name} values (3, '[{"a": {"c": 1}}, {"a": {"b": 2}}]');
            """
        sql_select_batch(table_name)
        sql_test_cast_to_array(table_name)
        sql_test_cast_to_scalar(table_name)

        // insert ambiguous structure for a which should throw exception
        test {
            sql """
                insert into ${table_name} values (4, '[{"a": {"c": 1}}, {"a": 2}]');
                """
            exception "Ambiguous structure of top_array nested subcolumns:"
        }

  
        // insert multi object in array
        sql """
            insert into ${table_name} values (5, '[{"a": {"c": 1}}, {"c": {"a": 2}}, {"b": "1"}]');
            """
        sql_select_batch(table_name)
        sql_test_cast_to_array(table_name)
        sql_test_cast_to_scalar(table_name)
        
        // insert multi b in array
        sql """
            insert into ${table_name} values (6, '[{"a": 1, "b": 1}, {"b": 2}, {"b": 3}]');
            """
        sql_select_batch(table_name)
        sql_test_cast_to_array(table_name)
        sql_test_cast_to_scalar(table_name)

        // trigger and wait compaction
        trigger_and_wait_compaction("${table_name}", "full")
        sql_select_batch(table_name)
        sql_test_cast_to_array(table_name)    
        sql_test_cast_to_scalar(table_name)

    } finally {
    }

}
