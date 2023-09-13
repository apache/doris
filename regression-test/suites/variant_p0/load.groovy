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

suite("regression_test_variant", "variant_type"){

    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
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

    def create_table = { table_name, buckets="auto", key_type="DUPLICATE" ->
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
    try {

        def key_types = ["DUPLICATE", "UNIQUE"]
        for (int i = 0; i < key_types.size(); i++) {
            def table_name = "simple_variant_${key_types[i]}"
            // 1. simple cases
            create_table.call(table_name, "auto", key_types[i])
            sql """insert into ${table_name} values (1,  '[1]'),(1,  '{"a" : 1}');"""
            sql """insert into ${table_name} values (2,  '[2]'),(1,  '{"a" : [[[1]]]}');"""
            sql """insert into ${table_name} values (3,  '3'),(1,  '{"a" : 1}'), (1,  '{"a" : [1]}');"""
            sql """insert into ${table_name} values (4,  '"4"'),(1,  '{"a" : "1223"}');"""
            sql """insert into ${table_name} values (5,  '5.0'),(1,  '{"a" : [1]}');"""
            sql """insert into ${table_name} values (6,  '"[6]"'),(1,  '{"a" : ["1", 2, 1.1]}');"""
            sql """insert into ${table_name} values (7,  '7'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
            sql """insert into ${table_name} values (8,  '8.11111'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
            sql """insert into ${table_name} values (9,  '"9999"'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
            sql """insert into ${table_name} values (10,  '1000000'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
            sql """insert into ${table_name} values (11,  '[123.0]'),(1999,  '{"a" : 1, "b" : {"c" : 1}}'),(19921,  '{"a" : 1, "b" : 10}');"""
            sql """insert into ${table_name} values (12,  '[123.2]'),(1022,  '{"a" : 1, "b" : 10}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
            qt_sql "select cast(v:a as array<int>) from  ${table_name} order by k"
            qt_sql_1 "select k, v from  ${table_name} order by k, cast(v as string)"
            qt_sql_1_1 "select k, v, cast(v:b as string) from  ${table_name} where  length(cast(v:b as string)) > 4 order  by k, cast(v as string)"
            // cast v:b as int should be correct
            // TODO FIX ME
            qt_sql_1_2 "select v:b, v:b.c, v from  ${table_name}  order by k desc limit 10000;"
            qt_sql_1_3 "select v:b from ${table_name} where cast(v:b as int) > 0;"
            qt_sql_1_4 "select k, v:b, v:b.c, v:a from ${table_name} where k > 10 order by k desc limit 10000;"
            qt_sql_1_5 "select cast(v:b as string) from ${table_name} order by k"
            verify table_name 
        }
        // FIXME
        sql "insert into simple_variant_DUPLICATE select k, cast(v as string) from simple_variant_UNIQUE;"
        
        // 2. type confilct cases
        def table_name = "type_conflict_resolution"
        create_table table_name
        sql """insert into ${table_name} values (1, '{"c" : "123"}');"""
        sql """insert into ${table_name} values (2, '{"c" : 123}');"""
        sql """insert into ${table_name} values (3, '{"cc" : [123]}');"""
        sql """insert into ${table_name} values (4, '{"cc" : [123.1]}');"""
        sql """insert into ${table_name} values (5, '{"ccc" : 123}');"""
        sql """insert into ${table_name} values (6, '{"ccc" : 123321}');"""
        sql """insert into ${table_name} values (7, '{"cccc" : 123.0}');"""
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
        qt_sql_2 "select v:A from ${table_name} order by cast(v:A as int)"
        sql """insert into ${table_name} values (12,  '{"AA" : [123456]}');"""
        sql """insert into ${table_name} values (14,  '{"AA" : [123456789101112]}');"""
        // qt_sql_3 "select v:AA from ${table_name} where size(v:AA) > 0 order by k"
        qt_sql_4 "select v:A, v:AA, v from ${table_name} order by k"
        qt_sql_5 "select v:A, v:AA, v, v from ${table_name} where cast(v:A as bigint) > 123 order by k"

        sql """insert into ${table_name} values (16,  '{"a" : 123.0, "A" : 191191, "c": 123}');"""
        sql """insert into ${table_name} values (18,  '{"a" : "123", "c" : 123456}');"""
        sql """insert into ${table_name} values (20,  '{"a" : 1.10111, "A" : 1800, "c" : [12345]}');"""
        // sql """insert into ${table_name} values (12,  '{"a" : [123]}, "c": "123456"');"""
        sql """insert into ${table_name} values (22,  '{"a" : 1.1111, "A" : 17211, "c" : 111111}');"""
        sql "sync"
        qt_sql_6 "select v:a, v:A from ${table_name} order by cast(v:A as bigint), k"
        qt_sql_7 "select k, v:A from ${table_name} where cast(v:A as bigint) >= 1 order by cast(v:A as bigint), k"

        // TODO: if not cast, then v:a could return "123" or 123 which is none determinately
        qt_sql_8 "select cast(v:a as string), v:A from ${table_name} where cast(v:a as json) is null order by k"
        // qt_sql_9 "select cast(v:a as string), v:A from ${table_name} where cast(v:A as json) is null order by k"

        // !!! Not found cast function String to Float64
        // qt_sql_10 "select v:a, v:A from ${table_name} where cast(v:a as double) > 0 order by k"
        qt_sql_11 "select v:A from ${table_name} where cast(v:A as bigint) > 1 order by k"

        // ----%%----
        qt_sql_12 "select v:A, v from ${table_name} where cast(v:A as bigint) > 1 order by k"
        // ----%%----
        qt_sql_13 "select v:a, v:A from simple_select_variant where 1=1 and cast(v:a as json) is null  and cast(v:A as bigint) >= 1  order by k;"
        qt_sql_14 """select  v:a, v:A, v from simple_select_variant where cast(v:A as bigint) > 0 and cast(v:A as bigint) = 123456 limit 1;"""

        // !!! Not found cast function String to Float64
        // qt_sql_15 "select v:a, v:A from ${table_name} where 1=1 and  cast(v:a as double) > 0 and v:A is not null  order by k"
        // qt_sql_16 "select v:a, v:A, v:c from ${table_name} where 1=1 and  cast(v:a as double) > 0 and v:A is not null  order by k"

        // TODO: if not cast, then v:a could return "123" or 123 which is none determinately 
        qt_sql_17 "select cast(v:a as json), v:A, v, v:AA from simple_select_variant where cast(v:A as bigint) is null  order by k;"

        sql """insert into simple_select_variant values (12, '{"oamama": 1.1}')"""
        qt_sql_18 "select  v:a, v:A, v, v:oamama from simple_select_variant where cast(v:oamama as double) is null  order by k;"
        qt_sql_19 """select  v:a, v:A, v, v:oamama from simple_select_variant where cast(v:oamama as double) is not null  order by k"""
        qt_sql_20 """select v:A from simple_select_variant where cast(v:A as bigint) > 0 and cast(v:A as bigint) = 123456 limit 1;"""

        // !!! Not found cast function String to Float64
        // qt_sql_21 """select v:A, v:a, v from simple_select_variant where cast(v:A as bigint)  > 0 and cast(v:a as double) > 1 order by cast(v:A as bigint);"""

        sql "truncate table simple_select_variant"
        sql """insert into simple_select_variant values (11, '{"x": [123456]}');"""
        sql """insert into simple_select_variant values (12, '{"x": [123456789101112]}');"""
        sql """insert into simple_select_variant values (12, '{"xxx" : 123, "yyy" : 456}');"""
        qt_sql_21_1 """select  * from simple_select_variant where cast(v:x as json) is null"""
        qt_sql_21_2 """select  cast(v:x as json)  from simple_select_variant where cast(v:x as json) is not null order by k;"""

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
        qt_sql_22 "select v1:A from multi_variant order by k;"
        qt_sql_23 "select v2:D from multi_variant order by k;"
        qt_sql_24 "select v2:C from multi_variant order by k;"

        // 5. multi tablets concurrent load
        table_name = "t_json_parallel"
        create_table table_name
        sql """INSERT INTO t_json_parallel SELECT *, '{"k1":1, "k2": "some", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}' FROM numbers("number" = "50000");"""
        qt_sql_25 """ SELECT sum(cast(v:k1 as int)), sum(cast(v:k4 as double)), sum(cast(json_extract(v:k5, "\$.[0].[0]") as int)) from t_json_parallel; """
            //50000  61700000        55000.00000000374       6150000
        // 7. gh data
        table_name = "ghdata"
        create_table table_name
        load_json_data.call(table_name, """${getS3Url() + '/load/ghdata_sample.json'}""")
        qt_sql_26 "select count() from ${table_name}"

        // 8. json empty string
        // table_name = "empty_string"
        // create_table table_name
        // sql """INSERT INTO empty_string VALUES (1, ''), (2, '{"k1": 1, "k2": "v1"}'), (3, '{}'), (4, '{"k1": 2}');"""
        // sql """INSERT INTO empty_string VALUES (3, null), (4, '{"k1": 1, "k2": "v1"}'), (3, '{}'), (4, '{"k1": 2}');"""
        // qt_sql_27 "SELECT * FROM ${table_name} ORDER BY k;"

        // // 9. btc data
        // table_name = "btcdata"
        // create_table table_name
        // load_json_data.call(table_name, """${getS3Url() + '/load/btc_transactions.json'}""")
        // qt_sql_28 "select count() from ${table_name}"

        // 10. alter add variant
        table_name = "alter_variant"
        create_table table_name
        sql """INSERT INTO ${table_name} VALUES (1, ''), (1, '{"k1": 1, "k2": "v1"}'), (1, '{}'), (1, '{"k1": 2}');"""
        sql "alter table ${table_name} add column v2 variant default null"
        sql """INSERT INTO ${table_name} VALUES (1, '{"kyyyy" : "123"}', '{"kxkxkxkx" : [123]}'), (1, '{"kxxxx" : 123}', '{"xxxxyyyy": 123}');"""
        qt_sql_29_1 """select * from alter_variant where length(cast(v2 as string)) > 2 order by k, cast(v as string), cast(v2 as string);"""
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
        sql """insert into ${table_name} values (2, '{"a" : ["123"]}')"""
        sql """insert into ${table_name} values (3, '{"a" : "123"}')"""
        sql """insert into ${table_name} values (4, '{"a" : 123456}')"""
        sql """insert into ${table_name} values (5, '{"a" : [123, "123", 1.11111]}')"""
        sql """insert into ${table_name} values (6, '{"a" : [123, 1.11, "123"]}')"""
        sql """insert into ${table_name} values(7, '{"a" : [123, {"xx" : 1}], "b" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        // TODO data bellow is invalid at present
        // sql """insert into ${table_name} values (8, '{"a" : [123, 111........]}')"""
        sql """insert into ${table_name} values (9, '{"a" : [123, {"a" : 1}]}')"""
        sql """insert into ${table_name} values (10, '{"a" : [{"a" : 1}, 123]}')"""
        qt_sql_29 "select v:a from ${table_name} order by k"
        // b? 7.111  [123,{"xx":1}]  {"b":{"c":456,"e":7.111}}       456
        qt_sql_30 "select v:b.e, v:a, v:b, v:b.c from jsonb_values where cast(v:b.e as double) > 1;"

        // 13. sparse columns
        table_name = "sparse_columns"
        create_table table_name
        sql """insert into  sparse_columns select 0, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 0, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_30 """ select v from sparse_columns where v is not null and json_extract(v, "\$") != "{}" order by cast(v as string) limit 10"""
        sql "truncate table sparse_columns"
        sql """insert into  sparse_columns select 0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}'  as json_str
            union  all select 0, '{"a" : 1234, "xxxx" : "kaana", "ddd" : {"aaa" : 123, "mxmxm" : [456, "789"]}}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_31 """ select v from sparse_columns where v is not null and json_extract(v, "\$") != "{}" order by cast(v as string) limit 10"""
        sql "truncate table sparse_columns"

        // 12. streamload remote file
        table_name = "logdata"
        create_table.call(table_name, "4")
        sql "set enable_two_phase_read_opt = false;"
        // no sparse columns
        set_be_config.call("ratio_of_defaults_as_sparse_column", "1")
        load_json_data.call(table_name, """${getS3Url() + '/load/logdata.json'}""")
        qt_sql_32 """ select v->"\$.json.parseFailed" from logdata where  v->"\$.json.parseFailed" != 'null' order by k limit 1;"""
        qt_sql_32_1 """select v:json.parseFailed from  logdata where cast(v:json.parseFailed as string) is not null and k = 162 limit 1;"""
        sql "truncate table ${table_name}"

        // 0.95 default ratio    
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
        load_json_data.call(table_name, """${getS3Url() + '/load/logdata.json'}""")
        qt_sql_33 """ select v->"\$.json.parseFailed" from logdata where  v->"\$.json.parseFailed" != 'null' order by k limit 1;"""
        qt_sql_33_1 """select v:json.parseFailed from  logdata where cast(v:json.parseFailed as string) is not null and k = 162 limit 1;"""
        sql "truncate table ${table_name}"

        // always sparse column
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0")
        load_json_data.call(table_name, """${getS3Url() + '/load/logdata.json'}""")
        qt_sql_34 """ select v->"\$.json.parseFailed" from logdata where  v->"\$.json.parseFailed" != 'null' order by k limit 1;"""
        sql "truncate table ${table_name}"
        qt_sql_35 """select v->"\$.json.parseFailed"  from logdata where k = 162 and  v->"\$.json.parseFailed" != 'null';"""
        qt_sql_35_1 """select v:json.parseFailed from  logdata where cast(v:json.parseFailed as string) is not null and k = 162 limit 1;"""

        // TODO add test case that some certain columns are materialized in some file while others are not materilized(sparse)
         // unique table
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
        table_name = "github_events_unique"
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
        sql "select * from ${table_name}"
        qt_sql_36_1 "select v:a, v:b, v:c from ${table_name} order by k limit 10"
        sql "DELETE FROM ${table_name} WHERE k=1"
        sql "select * from ${table_name}"
        qt_sql_36 "select * from ${table_name} where k > 3 order by k desc limit 10"

        // delete sign
        load_json_data.call(table_name, """delete.json""")

        // filter invalid variant
        table_name = "invalid_variant"
        set_be_config.call("max_filter_ratio_for_variant_parsing", "1")
        create_table.call(table_name, "4")
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a"  1}')""" 
        sql """insert into ${table_name} values (1, '{"a"  1}'), (1, '{"a"  1}')""" 
        set_be_config.call("max_filter_ratio_for_variant_parsing", "0.05")
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a"  1}')""" 
        sql """insert into ${table_name} values (1, '{"a"  1}'), (1, '{"a"  1}')""" 
        sql "select * from ${table_name}"

        // test all sparse columns
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0")
        table_name = "all_sparse_columns"
        create_table.call(table_name, "1")
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a":  "1"}')""" 
        sql """insert into ${table_name} values (1, '{"a" : 1}'), (1, '{"a":  ""}')""" 
        qt_sql_37 "select * from ${table_name} order by k, cast(v as string)"
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")

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
        qt_sql_38 "select * from ${table_name} order by k"

        // read text from sparse col
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0")
        sql """insert into  sparse_columns select 0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}'  as json_str
            union  all select 0, '{"a" : 1234, "xxxx" : "kaana", "ddd" : {"aaa" : 123, "mxmxm" : [456, "789"]}}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_31 """select cast(v:xxxx as string) from sparse_columns where cast(v:xxxx as string) != 'null' limit 1;"""
        sql "truncate table sparse_columns"
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")

        // test with inverted index
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0")
        set_be_config.call("threshold_rows_to_estimate_sparse_column", "0")
        sql "DROP TABLE IF EXISTS var_index"
        sql """
            CREATE TABLE IF NOT EXISTS var_index (
                k bigint,
                v variant,
                inv string,
                INDEX idx(inv) USING INVERTED PROPERTIES("parser"="standard")  COMMENT ''
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1 
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
        sql """insert into var_index values(1, '{"a" : 0, "b": 3}', 'hello world'), (2, '{"a" : 123}', 'world'),(3, '{"a" : 123}', 'hello world')"""
        qt_sql_inv_1 "select v:a from var_index where inv match 'hello' order by k"
        qt_sql_inv_2 "select v:a from var_index where inv match 'hello' and cast(v:a as int) > 0 order by k"
        qt_sql_inv_3 "select * from var_index where inv match 'hello' and cast(v:a as int) > 0 order by k"

    } finally {
        // reset flags
        set_be_config.call("max_filter_ratio_for_variant_parsing", "0.05")
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
    }
}
