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

// this test is used to test the load of nested array
suite("variant_nested_type_load", "p0"){

    try {

        // create a table with conflict variant which insert same nested subcolumn and scalar subcolumn data 
        def table_name = "var_nested_load_conflict"
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """set describe_extend_variant_column = true"""

        // set disable_variant_flatten_nested = true to disable variant flatten nested which is default behavior
        sql """ set disable_variant_flatten_nested = true """
        test {
            sql """
                    CREATE TABLE IF NOT EXISTS ${table_name} (
                        k bigint,
                        v variant
                    )
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 -- 1 bucket make really compaction in conflict case
                    properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true");
                """
            exception "If you want to enable variant flatten nested, please set session variable"
        }
        

        // set disable_variant_flatten_nested = false to enable variant flatten nested
        sql """ set disable_variant_flatten_nested = false """
        sql """
                    CREATE TABLE IF NOT EXISTS ${table_name} (
                        k bigint,
                        v variant
                    )
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 -- 1 bucket make really compaction in conflict case
                    properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true");
                """
        sql """ insert into ${table_name} values (1, '{"nested": [{"a": 1, "c": 1.1}, {"b": "1"}]}'); """ 
        
        def desc_table = { tn ->
            sql """ set describe_extend_variant_column = true """
            sql """ select * from ${tn} order by k """
            qt_sql_desc """ desc ${tn} """
        }

       def sql_select_batch = { tn ->
            qt_sql_0 """select * from ${tn} order by k"""

            qt_sql_1 """select v['nested']['a'] from ${tn} order by k"""
            qt_sql_2 """select v['nested']['b'] from ${tn} order by k"""
            qt_sql_3 """select v['nested']['c'] from ${tn} order by k"""

            qt_sql_4 """select v['nested'] from ${tn} order by k"""
        }

        def sql_test_cast_to_array = { tn ->
            // test cast to array<int> 
            qt_sql_8 """select cast(v['nested']['a'] as array<int>), size(cast(v['nested']['a'] as array<int>)) from ${tn} order by k"""
            qt_sql_9 """select cast(v['nested']['b'] as array<int>), size(cast(v['nested']['b'] as array<int>)) from ${tn} order by k"""
            qt_sql_10 """select cast(v['nested']['c'] as array<int>), size(cast(v['nested']['c'] as array<int>)) from ${tn} order by k"""

            // test cast to array<string> 
            qt_sql_11 """select cast(v['nested']['a'] as array<string>), size(cast(v['nested']['a'] as array<string>)) from ${tn} order by k"""
            qt_sql_12 """select cast(v['nested']['b'] as array<string>), size(cast(v['nested']['b'] as array<string>)) from ${tn} order by k"""
            qt_sql_13 """select cast(v['nested']['c'] as array<string>), size(cast(v['nested']['c'] as array<string>)) from ${tn} order by k"""

            // test cast to array<double> 
            qt_sql_14 """select cast(v['nested']['a'] as array<double>), size(cast(v['nested']['a'] as array<double>)) from ${tn} order by k"""
            qt_sql_15 """select cast(v['nested']['b'] as array<double>), size(cast(v['nested']['b'] as array<double>)) from ${tn} order by k"""
            qt_sql_16 """select cast(v['nested']['c'] as array<double>), size(cast(v['nested']['c'] as array<double>)) from ${tn} order by k"""

        }

        def sql_test_cast_to_scalar = { tn ->
            qt_sql_17 """select cast(v['nested']['a'] as int), cast(v['nested']['b'] as int), cast(v['nested']['c'] as int) from ${tn} order by k"""
            qt_sql_18 """select cast(v['nested']['a'] as string), cast(v['nested']['b'] as string), cast(v['nested']['c'] as string) from ${tn} order by k"""
            qt_sql_19 """select cast(v['nested']['a'] as double), cast(v['nested']['b'] as double), cast(v['nested']['c'] as double) from ${tn} order by k"""
        }

        /// insert a array of object for a, b, c 
        // insert structure conflict in one row
        //  a , b, c is Nested array,
        def table_name_1 = "var_nested_load_no_conflict"
        sql "DROP TABLE IF EXISTS ${table_name_1}"
        sql """set describe_extend_variant_column = true"""
        sql """
                CREATE TABLE IF NOT EXISTS ${table_name_1} (
                    k bigint,
                    v variant
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1 -- 1 bucket make really compaction in conflict case
                properties("replication_num" = "1", "disable_auto_compaction" = "true", "variant_enable_flatten_nested" = "true");
            """
        // insert a array of object for a, b, c first then insert structure conflict in one row
        // insert structure conflict in one row
        //  a , b, c is Nested array,
        sql """
            insert into ${table_name_1} values (1, '{"nested": [{"a": 1, "c": 1.1}, {"b": "1"}]}'); 
            """
        sql_select_batch(table_name_1)
        sql_test_cast_to_array(table_name_1)
        sql_test_cast_to_scalar(table_name_1)
        // insert structure conflict in one row
        test {
            sql """
                insert into ${table_name_1} values (2, '{"nested": {"a": 2.5, "b": "123.1"}}');
                """
            exception "Ambiguous paths"
        }
        // insert more different combination data for a, b, c
        sql """
            insert into ${table_name_1} values (3, '{"nested": [{"a": 2.5, "b": "123.1"}]}');
            """
        sql """
            insert into ${table_name_1} values (4, '{"nested": [{"a": 2.5, "b": 123.1}]}');
            """
        sql """
            insert into ${table_name_1} values (5, '{"nested": [{"a": 2.5, "c": "123.1"}, {"b": "123.1"}]}');
            """
        sql """
            insert into ${table_name_1} values (6, '{"nested": [{"a": 2.5}, {"b": 123.1}]}');
            """
        sql """
            insert into ${table_name_1} values (7, '{"nested": [{"a": 2.5}, {"c": 123.1}, {"b": "123.1"}]}');
                """
        // we should trigger compaction here to make sure the data is loaded correctly for v.nested.b/c: array<json>
        // trigger and wait compaction
        trigger_and_wait_compaction("${table_name_1}", "full")
        sql_select_batch(table_name_1)
        sql_test_cast_to_array(table_name_1)    
        sql_test_cast_to_scalar(table_name_1)

        // drop table
        sql """ drop table ${table_name_1} """
        sql """ create table ${table_name_1} (k bigint, v variant) duplicate key(k) distributed by hash(k) buckets 1 properties("replication_num" = "1", "disable_auto_compaction" = "true", "variant_enable_flatten_nested" = "true") """
        // insert scalar data first then insert structure conflict in one row
        sql """
            insert into ${table_name_1} values (1, '{"nested": {"a": 2.5, "b": "123.1"}}');
            """
        sql_select_batch(table_name_1)
        sql_test_cast_to_array(table_name_1)
        sql_test_cast_to_scalar(table_name_1)
        // insert structure conflict in one row:  a array of object for a, b, c
        test {
            sql """
                insert into ${table_name_1} values (2, '{"nested": [{"a": 2.5, "b": "123.1"}]}');
                """
            exception "Ambiguous paths"
        }
        // insert more different combination data for a, b, c in scalar
        sql """
            insert into ${table_name_1} values (3, '{"nested": {"a": 2.5, "b": 123.1}}');
            """
        sql """
            insert into ${table_name_1} values (4, '{"nested": {"a": 2.5, "c": "123.1"}}');
            """
        sql """
            insert into ${table_name_1} values (5, '{"nested": {"a": 2.5, "c": 123.1}}');
            """
        sql """
            insert into ${table_name_1} values (6, '{"nested": {"a": 2.5, "c": "123.1"}}');
            """
        sql """
            insert into ${table_name_1} values (7, '{"nested": {"a": 2.5, "b": "123.1", "c": 123.1}}');
            """
        // we should trigger compaction here to make sure the data is loaded correctly for v.nested.b/c: json
        // trigger and wait compaction
        trigger_and_wait_compaction("${table_name_1}", "full")
        sql_select_batch(table_name_1)
        sql_test_cast_to_array(table_name_1)    
        sql_test_cast_to_scalar(table_name_1)

    } finally {
    }

}
