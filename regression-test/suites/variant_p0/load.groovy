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

suite("regression_test_variant", "p0"){

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
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def verify = { table_name ->
        sql "sync"
        qt_sql """select count() from ${table_name}"""
    }

    def create_table = { table_name, key_type="DUPLICATE", buckets=(new Random().nextInt(15) + 1).toString()  ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            ${key_type} KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS ${buckets}
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }

    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }
    sql "set experimental_enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    try {
        def key_types = ["DUPLICATE", "UNIQUE"]
        for (int i = 0; i < key_types.size(); i++) {
            def table_name = "simple_variant_${key_types[i]}"
            // 1. simple cases
            create_table.call(table_name, key_types[i])
            sql """insert into ${table_name} values (1,  '[1]'),(1,  '{"a" : 1}');"""
            sql """insert into ${table_name} values (2,  '[2]'),(1,  '{"a" : [[[1]]]}');"""
            sql """insert into ${table_name} values (3,  '3'),(1,  '{"a" : 1}'), (1,  '{"a" : [1]}');"""
            sql """insert into ${table_name} values (4,  '"4"'),(1,  '{"a" : "1223"}');"""
            sql """insert into ${table_name} values (5,  '5'),(1,  '{"a" : [1]}');"""
            sql """insert into ${table_name} values (6,  '"[6]"'),(1,  '{"a" : ["1", 2, 1.1]}');"""
            sql """insert into ${table_name} values (7,  '7'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
            sql """insert into ${table_name} values (8,  '8.11111'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
            sql """insert into ${table_name} values (9,  '"9999"'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
            sql """insert into ${table_name} values (10,  '1000000'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
            sql """insert into ${table_name} values (11,  '[123.1]'),(1999,  '{"a" : 1, "b" : {"c" : 1}}'),(19921,  '{"a" : 1, "b" : 10}');"""
            sql """insert into ${table_name} values (12,  '[123.2]'),(1022,  '{"a" : 1, "b" : 10}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
            qt_sql1 "select k, cast(v['a'] as array<int>) from  ${table_name} where  size(cast(v['a'] as array<int>)) > 0 order by k, cast(v['a'] as string) asc"
            qt_sql2 "select k, cast(v as int), cast(v['b'] as string) from  ${table_name} where  length(cast(v['b'] as string)) > 4 order  by k, cast(v as string), cast(v['b'] as string) "
            qt_sql3 "select k, v from  ${table_name} order by k, cast(v as string) limit 5"
            qt_sql4 "select v['b'], v['b']['c'], cast(v as int) from  ${table_name} where cast(v['b'] as string) is not null and   cast(v['b'] as string) != '{}' order by k,cast(v as string) desc limit 10000;"
            qt_sql5 "select v['b'] from ${table_name} where cast(v['b'] as int) > 0;"
            qt_sql6 "select cast(v['b'] as string) from ${table_name} where cast(v['b'] as string) is not null and   cast(v['b'] as string) != '{}' order by k,  cast(v['b'] as string) "
            // verify table_name 
        }
        sql "insert into simple_variant_DUPLICATE select k, cast(v as string) from simple_variant_UNIQUE;"
       
        // 2. type confilct cases
        def table_name = "type_conflict_resolution"
        create_table table_name
        sql """insert into ${table_name} values (1, '{"c" : "123"}');"""
        sql """insert into ${table_name} values (2, '{"c" : 123}');"""
        sql """insert into ${table_name} values (3, '{"cc" : [123.2]}');"""
        sql """insert into ${table_name} values (4, '{"cc" : [123.1]}');"""
        sql """insert into ${table_name} values (5, '{"ccc" : 123}');"""
        sql """insert into ${table_name} values (6, '{"ccc" : 123321}');"""
        sql """insert into ${table_name} values (7, '{"cccc" : 123.22}');"""
        sql """insert into ${table_name} values (8, '{"cccc" : 123.11}');"""
        sql """insert into ${table_name} values (9, '{"ccccc" : [123]}');"""
        sql """insert into ${table_name} values (10, '{"ccccc" : [123456789]}');"""
        sql """insert into ${table_name} values (11, '{"b" : 1111111111111111}');"""
        sql """insert into ${table_name} values (12, '{"b" : 1.222222}');"""
        sql """insert into ${table_name} values (13, '{"bb" : 1}');"""
        sql """insert into ${table_name} values (14, '{"bb" : 214748364711}');"""
        sql """insert into ${table_name} values (15, '{"A" : 1}');"""
        qt_sql """select v from type_conflict_resolution order by k;"""
        verify table_name

        // 3. simple variant sub column select
        table_name = "simple_select_variant"
        create_table table_name
        sql """insert into ${table_name} values (1,  '{"A" : 123}');"""
        sql """insert into ${table_name} values (2,  '{"A" : 1}');"""
        sql """insert into ${table_name} values (4,  '{"A" : 123456}');"""
        sql """insert into ${table_name} values (8,  '{"A" : 123456789101112}');"""
        qt_sql_2 "select v['A'] from ${table_name} order by cast(v['A'] as bigint)"
        sql """insert into ${table_name} values (12,  '{"AA" : [123456]}');"""
        sql """insert into ${table_name} values (14,  '{"AA" : [123456789101112]}');"""
        qt_sql_4 "select cast(v['A'] as string), v['AA'], v from ${table_name} order by k"
        qt_sql_5 "select v['A'], v['AA'], v, v from ${table_name} where cast(v['A'] as bigint) > 123 order by k"

        sql """insert into ${table_name} values (16,  '{"a" : 123.22, "A" : 191191, "c": 123}');"""
        sql """insert into ${table_name} values (18,  '{"a" : "123", "c" : 123456}');"""
        sql """insert into ${table_name} values (20,  '{"a" : 1.10111, "A" : 1800, "c" : [12345]}');"""
        // sql """insert into ${table_name} values (12,  '{"a" : [123]}, "c": "123456"');"""
        sql """insert into ${table_name} values (22,  '{"a" : 1.1111, "A" : 17211, "c" : 111111}');"""
        sql "sync"
        qt_sql_6 "select cast(v['a'] as string), v['A'] from ${table_name} order by cast(v['A'] as bigint), k"
        qt_sql_7 "select k, v['A'] from ${table_name} where cast(v['A'] as bigint) >= 1 order by cast(v['A'] as bigint), k"

        qt_sql_8 "select cast(v['a'] as string), v['A'] from ${table_name} where cast(v['a'] as json) is null order by k"

        qt_sql_11 "select v['A'] from ${table_name} where cast(v['A'] as bigint) > 1 order by k"

        // ----%%----
        qt_sql_12 "select v['A'], v from ${table_name} where cast(v['A'] as bigint) > 1 order by k"
        // ----%%----
        qt_sql_13 "select v['a'], v['A'] from simple_select_variant where 1=1 and cast(v['a'] as json) is null  and cast(v['A'] as bigint) >= 1  order by k;"
        qt_sql_14 """select v['A'], v from simple_select_variant where cast(v['A'] as bigint) > 0 and cast(v['A'] as bigint) = 123456 limit 1;"""

        sql """insert into simple_select_variant values (12, '{"oamama": 1.1}')"""
        qt_sql_18 "select  cast(v['a'] as text), v['A'], v, v['oamama'] from simple_select_variant where cast(v['oamama'] as double) is null  order by k;"
        qt_sql_19 """select  v['a'], v['A'], v, v['oamama'] from simple_select_variant where cast(v['oamama'] as double) is not null  order by k"""
        qt_sql_20 """select v['A'] from simple_select_variant where cast(v['A'] as bigint) > 0 and cast(v['A'] as bigint) = 123456 limit 1;"""

        sql "truncate table simple_select_variant"
        sql """insert into simple_select_variant values (11, '{"x": [123456]}');"""
        sql """insert into simple_select_variant values (12, '{"x": [123456789101112]}');"""
        sql """insert into simple_select_variant values (12, '{"xxx" : 123, "yyy" : 456}');"""
        qt_sql_21_1 """select  * from simple_select_variant where cast(v['x'] as json) is null"""
        qt_sql_21_2 """select  cast(v['x'] as json)  from simple_select_variant where cast(v['x'] as json) is not null order by k;"""

        // 4. multi variant in single table
        table_name = "multi_variant"
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
                CREATE TABLE IF NOT EXISTS ${table_name} (
                    k bigint,
                    v1 variant,
                    v2 variant,
                    v3 variant

                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY RANDOM BUCKETS 5 
                properties("replication_num" = "1", "disable_auto_compaction" = "false");
            """
        sql """insert into ${table_name} values (1,  '{"A" : 123}', '{"B" : 123}', '{"C" : 456}');"""
        sql """insert into ${table_name} values (2,  '{"C" : "123"}', '{"D" : [123]}', '{"E" : 789}');"""
        sql """insert into ${table_name} values (3,  '{"C" : "123"}', '{"C" : [123]}', '{"E" : "789"}');"""
        sql "sync"
        verify table_name
        qt_sql_22 "select v1['A'] from multi_variant order by k;"
        qt_sql_23 "select v2['D'] from multi_variant order by k;"
        qt_sql_24 "select v2['C'] from multi_variant order by k;"

        // 5. multi tablets concurrent load
        table_name = "t_json_parallel"
        create_table table_name
        sql """INSERT INTO t_json_parallel SELECT *, '{"k1":1, "k2": "some", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "50000");"""
        qt_sql_25 """ SELECT sum(cast(v['k1'] as int)), sum(cast(v['k4'] as double)), sum(cast(json_extract(v['k5'], "\$.[0].[0]") as int)) from t_json_parallel; """
            //50000  61700000        55000.00000000374       6150000
        // 7. gh data
        table_name = "ghdata"
        create_table table_name
        load_json_data.call(table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
        qt_sql_26 "select count() from ${table_name}"

        // 8. json empty string
        table_name = "empty_string"
        create_table table_name
        sql """INSERT INTO empty_string VALUES (1, ''), (2, '{"k1": 1, "k2": "v1"}'), (3, '{}'), (4, '{"k1": 2}');"""
        sql """INSERT INTO empty_string VALUES (3, null), (4, '{"k1": 1, "k2": "v1"}'), (3, '{}'), (4, '{"k1": 2}');"""
        sql """INSERT INTO empty_string VALUES (3, null), (4, null), (3, '{}'), (4, '{"k1": 2}');"""
        sql """INSERT INTO empty_string VALUES (3, ''), (4, null), (3, '{}'), (4, null);"""
        qt_sql_27 "SELECT count() FROM ${table_name};"

        // // // 9. btc data
        // // table_name = "btcdata"
        // // create_table table_name
        // // load_json_data.call(table_name, """${getS3Url() + '/regression/load/btc_transactions.json'}""")
        // // qt_sql_28 "select count() from ${table_name}"

        // 10. alter add variant
        table_name = "alter_variant"
        create_table table_name
        sql """INSERT INTO ${table_name} VALUES (1, ''), (1, '{"k1": 1, "k2": "v1"}'), (1, '{}'), (1, '{"k1": 2}');"""
        sql "alter table ${table_name} add column v2 variant default null"
        sql """INSERT INTO ${table_name} VALUES (1, '{"kyyyy" : "123"}', '{"kxkxkxkx" : [123]}'), (1, '{"kxxxx" : 123}', '{"xxxxyyyy": 123}');"""
        qt_sql_29_1 """select * from alter_variant where length(cast(v2 as string)) > 2 and cast(v2 as string) != 'null' order by k, cast(v as string), cast(v2 as string);"""
        verify table_name

        // 11. boolean values 
        table_name = "boolean_values"
        create_table table_name
        sql """INSERT INTO ${table_name} VALUES (1, ''), (2, '{"k1": true, "k2": false}'), (3, '{}'), (4, '{"k1": false}');"""
        verify table_name

        // 12. jsonb values
        table_name = "jsonb_values"
        create_table table_name
        sql """insert into ${table_name} values (1, '{"a" : ["123", 123, [123]]}')"""
        // FIXME array -> jsonb will parse error
        // sql """insert into ${table_name} values (2, '{"a" : ["123"]}')"""
        sql """insert into ${table_name} values (3, '{"a" : "123"}')"""
        sql """insert into ${table_name} values (4, '{"a" : 123456}')"""
        sql """insert into ${table_name} values (5, '{"a" : [123, "123", 1.11111]}')"""
        sql """insert into ${table_name} values (6, '{"a" : [123, 1.11, "123"]}')"""
        sql """insert into ${table_name} values(7, '{"a" : [123, {"xx" : 1}], "b" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        // FIXME data bellow is invalid at present
        // sql """insert into ${table_name} values (8, '{"a" : [123, 111........]}')"""
        sql """insert into ${table_name} values (9, '{"a" : [123, {"a" : 1}]}')"""
        sql """insert into ${table_name} values (10, '{"a" : [{"a" : 1}, 123]}')"""
        qt_sql_29 "select cast(v['a'] as string) from ${table_name} order by k"
        // b? 7.111  [123,{"xx":1}]  {"b":{"c":456,"e":7.111}}       456
        qt_sql_30 "select v['b']['e'], v['a'], v['b'], v['b']['c'] from jsonb_values where cast(v['b']['e'] as double) > 1;"

        // 13. sparse columns
        table_name = "sparse_columns"
        create_table table_name
        sql """insert into  sparse_columns select 0, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 0, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_30 """ select v from sparse_columns where json_extract(v, "\$") != "{}" order by cast(v as string) limit 10"""
        sql "truncate table sparse_columns"
        sql """insert into  sparse_columns select 0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}'  as json_str
            union  all select 0, '{"a" : 1234, "xxxx" : "kaana", "ddd" : {"aaa" : 123, "mxmxm" : [456, "789"]}}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_31 """ select v from sparse_columns where json_extract(v, "\$") != "{}" order by cast(v as string) limit 10"""
        sql "truncate table sparse_columns"

        table_name = "github_events"
        sql """DROP TABLE IF EXISTS ${table_name}"""
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 4
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """
        load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (2, '{"b" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (2, '{"c" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (3, '{"d" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (3, '{"e" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (4, '{"f" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (4, '{"g" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (5, '{"h" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (5, '{"i" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (6, '{"j" : 1}'), (1, '{"a" : 1}')"""
        sql """insert into ${table_name} values (6, '{"k" : 1}'), (1, '{"a" : 1}')"""
        sql "select /*+SET_VAR(batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=false,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_instance_num=1,parallel_pipeline_task_num=4,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=true,parallel_scan_max_scanners_count=16,parallel_scan_min_rows_per_scanner=128,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=2,enable_parallel_result_sink=false,sort_phase_num=0,enable_nereids_planner=true,rewrite_or_to_in_predicate_threshold=2,enable_function_pushdown=true,enable_common_expr_pushdown=true,enable_local_exchange=true,partitioned_hash_join_rows_threshold=1048576,partitioned_hash_agg_rows_threshold=8,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=true,enable_two_phase_read_opt=true,enable_common_expr_pushdown_for_inverted_index=false,enable_delete_sub_predicate_v2=true,min_revocable_mem=33554432,fetch_remote_schema_timeout_seconds=120,max_fetch_remote_schema_tablet_count=512,enable_join_spill=false,enable_sort_spill=false,enable_agg_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,external_agg_partition_bits=5) */ * from ${table_name}"
        qt_sql_36_1 "select cast(v['a'] as int), cast(v['b'] as int), cast(v['c'] as int) from ${table_name} order by k limit 10"
        sql "DELETE FROM ${table_name} WHERE k=1"
        sql "select * from ${table_name}"
        qt_sql_36_2 """select k, json_extract(cast(v as text), "\$.repo") from ${table_name} where k > 3 order by k desc limit 10"""
        sql "insert into ${table_name} select * from ${table_name}"
        sql """UPDATE ${table_name} set v = '{"updated_value" : 10}' where k = 2"""
        qt_sql_36_3 """select * from ${table_name} where k = 2"""

        // delete sign
        load_json_data.call(table_name, """delete.json""")

        // FIXME
        // // filter invalid variant
        // table_name = "invalid_variant"
        // set_be_config.call("max_filter_ratio_for_variant_parsing", "1")
        // create_table.call(table_name,  "DUPLICATE", "4")
        // sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a"  1}')""" 
        // sql """insert into ${table_name} values (1, '{"a"  1}'), (1, '{"a"  1}')""" 
        // set_be_config.call("max_filter_ratio_for_variant_parsing", "0.05")
        // sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a"  1}')""" 
        // sql """insert into ${table_name} values (1, '{"a"  1}'), (1, '{"a"  1}')""" 
        // sql "select * from ${table_name}"

        // test all sparse columns
        table_name = "all_sparse_columns"
        create_table.call(table_name,  "DUPLICATE", "1")
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a":  "1"}')""" 
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a":  "2"}')""" 
        qt_sql_37 "select * from ${table_name} order by k, cast(v as string)"

        // test mow with delete
        table_name = "variant_mow" 
        sql """
         CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                k1 string,
                v variant
            )
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 4 
            properties("replication_num" = "1", "disable_auto_compaction" = "false", "enable_unique_key_merge_on_write" = "true");
        """
        sql """insert into ${table_name} values (1, "abc", '{"a" : 1}'), (1, "cde", '{"b" : 1}')"""
        sql """insert into ${table_name} values (2, "abe", '{"c" : 1}')"""
        sql """insert into ${table_name} values (3, "abd", '{"d" : 1}')"""
        sql "delete from ${table_name} where k in (select k from variant_mow where k in (1, 2))"
        qt_sql_38 "select * from ${table_name} order by k limit 10"

        // read text from sparse col
        sql """insert into  sparse_columns select 0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}'  as json_str
            union  all select 0, '{"a" : 1234, "xxxx" : "kaana", "ddd" : {"aaa" : 123, "mxmxm" : [456, "789"]}}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_31 """select cast(v['xxxx'] as string) from sparse_columns where cast(v['xxxx'] as string) != 'null' order by k limit 1;"""
        sql "truncate table sparse_columns"

        // test cast
        table_name = "variant_cast"         
        create_table.call(table_name,  "DUPLICATE", "1")
        sql """
            insert into variant_cast values(1,'["CXO0N: 1045901740", "HMkTa: 1348450505", "44 HHD: 915015173", "j9WoJ: -1517316688"]'),(2,'"[1]"'),(3,'123456'),(4,'1.11111')
        """
        qt_sql_39 "select k, json_type(cast(v as json), '\$')  from variant_cast order by k" 
        qt_sql_39 "select cast(v as array<text>)  from variant_cast where k = 1 order by k" 
        qt_sql_39 "select cast(v as string)  from variant_cast where k = 2 order by k" 

        sql "DROP TABLE IF EXISTS records"
        sql """
            CREATE TABLE `records` (
                  `id` VARCHAR(20) NOT NULL,
                  `entity_id` VARCHAR(20) NOT NULL,
                  `value` VARIANT NOT NULL,
                  INDEX idx_value (`value`) USING INVERTED PROPERTIES("parser" = "unicode", "lower_case" = "true") COMMENT 'inverted index for value'
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`id`) BUCKETS 10
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "min_load_replica_num" = "-1",
                "is_being_synced" = "false",
                "storage_medium" = "hdd",
                "storage_format" = "V2",
                "inverted_index_storage_format" = "V1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true",
                "disable_auto_compaction" = "false",
                "enable_single_replica_compaction" = "false",
                "group_commit_interval_ms" = "10000",
                "group_commit_data_bytes" = "134217728"
                );
        """
        sql """
            insert into records values ('85321037218054145', 'A100', '{"id":"85321037218054145","id0":"8301","id12":"32030","id16":"39960","id20":"17202","id24":"24592","id28":"42035","id32":"29819","id36":"4680","id4":"4848","id40":"47892","id44":"29400","id48":"7799","id52":"49678","id56":"40585","id60":"23572","id64":"28579","id68":"11477","id72":"35416","id76":"9577","id8":"25758","id80":"45204","id84":"16132","id88":"1007","id92":"32630","id96":"15443","num10":310671794,"num14":317675907,"num18":173663246,"num2":68835462,"num22":919923967,"num26":989144179,"num30":758415664,"num34":344178710,"num38":603490103,"num42":928353352,"num46":164440235,"num50":272803033,"num54":494457109,"num58":36023952,"num6":345965722,"num62":244316054,"num66":791098758,"num70":59531230,"num74":887460141,"num78":175760447,"num82":93180735,"num86":893826383,"num90":899738404,"num94":132357718,"num98":618243870,"text11":"1露华浓 蜜丝佛陀 欧莱雅 哪 款 粉饼 好啊我 感觉 ","text15":"0写 个 天秤 女 攻略 吧 转帖楼主 救 我 加急 如","text19":"1我 复试 被 人大 刷 了 我 气 得 浑身 颤抖 请 ","text23":"0哇靠 我 是 昨儿 吐槽 太仓 假货 的 妹子百丽 专","text27":"1搓 泥 现象 是什么 原因换 成 乳液可是 早上 还","text3":"1想说 刘伊心 是 来 搞笑 的 吗能 唱 成 她 这样","text31":"0女朋友 发 来 短信 说 离开 我 是 她 最 正确 的","text35":"1做 完 九 次 led 红 蓝光 了 还有 最后 一次 ","text39":"0八一八 你 觉得 哪 位 明星 长 得 很 苦逼 很 苦","text43":"0昨天 楼主 拒绝 了 一个 女生 我 傻逼 吗手机 看","text47":"0818 你 觉得 超 好看 的 动漫 有 哪些 嘛初中","text51":"0借 你 我 的 时间我 能 说 请 豆油 我 吗不行","text55":"1我 在 缅甸 雪 悟 敏 禅修 营 三 个 月谢谢 于","text59":"0八一八 非常了得 吧孟非 明显 是 一 付 很 嫌弃 ","text63":"0我 小 姨 大 病 之后 神仙 附体 之后 几乎 是 一","text67":"0爸爸 去哪儿 明星 家 也是 有 贫富悬殊 的 哇突然","text7":"0曾经 的 成都 洗面桥 小学如果 大家 还 记得 宋 ","text71":"1关于女人 姿色女 活 白富美 音 智 胸 腰腿声音 ","text75":"1直播 贴 2013 02 19 04 00 足总 杯 第","text79":"1昨天 答应 和 ex 见面 了 终于 又 让 他 知道 ","text83":"1谁 能 提供 指 人 儿 的 gtp 谱子 呢 我们 乐","text87":"0近 距离 安 坏 关系我 和 我 儿子 是 近 距离 ","text91":"1刚 看 了 小 时代 1 感动 了我 也是 我 都 3","text95":"0剧终 谢谢 大家 撸 主 和 她 在一起 了我 呸呸 ","text99":"1喜欢 广州 想来 广州 但是嗯嗯 还 不能 在 网上 ","time1":"2012-07-06 17:42:41","time13":"1985-07-23 13:16:34","time17":"1970-07-14 00:30:54","time21":"1981-07-04 03:01:10","time25":"1994-12-20 03:41:44","time29":"1975-06-25 07:25:00","time33":"1979-07-27 03:59:56","time37":"1979-12-25 00:37:57","time41":"1979-11-12 13:09:03","time45":"1998-03-02 15:41:07","time49":"1981-05-22 06:40:29","time5":"1995-02-26 15:11:35","time53":"1985-06-23 23:11:00","time57":"2001-02-05 14:44:11","time61":"1988-01-27 12:28:13","time65":"2024-04-27 04:52:59","time69":"2022-04-18 05:19:02","time73":"2016-03-14 15:55:31","time77":"1977-12-21 23:41:05","time81":"2016-12-02 17:10:12","time85":"1995-09-15 21:40:33","time89":"2014-05-10 14:32:32","time9":"1988-12-08 05:26:13","time93":"2018-08-23 02:01:29","time97":"1990-03-09 07:39:01"}');
        """
        qt_sql_records1 """SELECT value FROM records WHERE  value['text3']  MATCH_ALL '刘伊心 是 来 搞笑 的'  OR ( value['text83']  MATCH_ALL '攻略 吧 转帖楼主 救' ) OR (  value['text15']  MATCH_ALL '个 天秤 女 攻略 吧 转帖楼主 ' )  LIMIT 0, 100"""
        qt_sql_records2 """SELECT value FROM records WHERE entity_id = 'A100'  and  value['id16'] = '39960' AND (  value['text59'] = '非 明显 是 一 付 很 嫌') AND (  value['text99'] = '来 广州 但是嗯嗯 还 不能 在')  LIMIT 0, 100;"""
        qt_sql_records3 """SELECT value FROM records WHERE   value['text99'] MATCH_ALL '来 广州 但是嗯嗯 还 不能 在'  OR (  value['text47'] MATCH_ALL '你 觉得 超 好看 的 动' ) OR (  value['text43'] MATCH_ALL ' 楼主 拒绝 了 一个 女生 我 傻逼 吗手' )  LIMIT 0, 100"""
        qt_sql_records4 """SELECT value FROM records WHERE  value['id16'] = '39960' AND (  value['text59'] = '非 明显 是 一 付 很 嫌') AND (  value['text99'] = '来 广州 但是嗯嗯 还 不能 在 ')  """
        qt_sql_records5 """SELECT value FROM records WHERE  value['text3'] MATCH_ALL '伊心 是 来 搞笑 的'  LIMIT 0, 100"""

        test {
            sql "select v['a'] from ${table_name} group by v['a']"
            exception("errCode = 2, detailMessage = Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function, and don't support filter, group by or order by")
        }

        test {
            sql """
            create table var(
                `key` int,
                `content` variant
            )
            DUPLICATE KEY(`key`)
            distributed by hash(`content`) buckets 8
            properties(
              "replication_allocation" = "tag.location.default: 1"
            );
            """
            exception("errCode = 2, detailMessage = Hash distribution info should not contain variant columns")
        }

         test {
            sql """
            CREATE TABLE `var_as_key` (
              `key` int NULL,
              `var` variant NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`key`, `var`)
            COMMENT 'OLAP'
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
            """ 
            exception("errCode = 2, detailMessage = Variant type should not be used in key")
        }
        sql "DROP TABLE IF EXISTS var_as_key"
        sql """
            CREATE TABLE `var_as_key` (
                `k` int NULL,
                `var` variant NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        sql """insert into var_as_key values(1, '{"a" : 10}')"""
        sql """insert into var_as_key values(2, '{"b" : 11}')"""
        qt_sql "select * from var_as_key order by k"

        test {
            sql """select * from ghdata where cast(v['actor']['url'] as ipv4) = '127.0.0.1'""" 
            exception("Invalid type for variant column: 36")
        }

    } finally {
        // reset flags
    }
}
