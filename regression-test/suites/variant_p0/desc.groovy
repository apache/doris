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

suite("regression_test_variant_desc", "nonConcurrent"){

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

    def create_table = { table_name, buckets="auto" ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS ${buckets}
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }

    def create_table_partition = { table_name, buckets="auto" ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            PARTITION BY RANGE(k)
            (
                PARTITION p1 VALUES LESS THAN (3000),
                PARTITION p2 VALUES LESS THAN (50000),
                PARTITION p3 VALUES LESS THAN (100000)
            )
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
        // sparse columns
        def table_name = "sparse_columns"
        create_table table_name
        set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "0.95")
        sql """set describe_extend_variant_column = true"""
        sql """insert into  sparse_columns select 0, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 0, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_1 """desc ${table_name}"""
        sql "truncate table sparse_columns"
        sql """insert into  sparse_columns select 0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}'  as json_str
            union  all select 0, '{"a" : 1234, "xxxx" : "kaana", "ddd" : {"aaa" : 123, "mxmxm" : [456, "789"]}}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_2 """desc ${table_name}"""
        sql "truncate table sparse_columns"

        // no sparse columns
        table_name = "no_sparse_columns"
        create_table.call(table_name, "4")
        sql "set enable_two_phase_read_opt = false;"
        set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1.0")
        sql """insert into  ${table_name} select 0, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 0, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        qt_sql_3 """desc ${table_name}"""
        sql "truncate table ${table_name}"

        // partititon
        table_name = "partition_data"
        create_table_partition.call(table_name, "4")
        sql "set enable_two_phase_read_opt = false;"
        set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "0.95")
        sql """insert into  ${table_name} select 2500, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}'  as json_str
            union  all select 2500, '{"a" : 1234, "xxxx" : "kaana", "ddd" : {"aaa" : 123, "mxmxm" : [456, "789"]}}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        sql """insert into  ${table_name} select 45000, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 45000, '{"a": 1123}' as json_str union all select 45000, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        sql """insert into  ${table_name} values(95000, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        qt_sql_6_1 """desc ${table_name} partition p1"""
        qt_sql_6_2 """desc ${table_name} partition p2"""
        qt_sql_6_3 """desc ${table_name} partition p3"""
        qt_sql_6 """desc ${table_name}"""
        sql "truncate table ${table_name}"

        // drop partition
        table_name = "drop_partition"
        create_table_partition.call(table_name, "4")
        // insert into partition p1
        sql """insert into  ${table_name} values(2500, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        // insert into partition p2
        sql """insert into  ${table_name} values(45000, '{"a": 11245, "xxxx" : "kaana"}')"""
        // insert into partition p3
         sql """insert into  ${table_name} values(95000, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        // drop p1
        sql """alter table ${table_name} drop partition p1"""
        qt_sql_7 """desc ${table_name}"""
        qt_sql_7_1 """desc ${table_name} partition p2"""
        qt_sql_7_2 """desc ${table_name} partition p3"""
        qt_sql_7_3 """desc ${table_name} partition (p2, p3)"""
        sql "truncate table ${table_name}"

        // more variant
        table_name = "more_variant_table"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v1 variant,
                v2 variant,
                v3 variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 5
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
        sql """ insert into ${table_name} values (0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}', '{"a": 11245, "xxxx" : "kaana"}', '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}')"""
        qt_sql_8 """desc ${table_name}"""
        sql "truncate table ${table_name}"

        // describe_extend_variant_column = false
        sql """set describe_extend_variant_column = false"""
        table_name = "no_extend_variant_column"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 5
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
        sql """ insert into ${table_name} values (0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}')"""
        qt_sql_9 """desc ${table_name}"""
        sql """set describe_extend_variant_column = true"""
        qt_sql_9_1 """desc ${table_name}"""
        sql "truncate table ${table_name}"

        // schema change: add varaint
        table_name = "schema_change_table"
        create_table.call(table_name, "5")
        // add, drop columns
        sql """INSERT INTO ${table_name} values(0, '{"k1":1, "k2": "hello world", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]]}')"""
        sql """set describe_extend_variant_column = true"""
        qt_sql_10 """desc ${table_name}"""
        // add column
        sql "alter table ${table_name} add column v2 variant default null"
        sql """ insert into ${table_name} values (0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}',
                 '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}')"""
        qt_sql_10_1 """desc ${table_name}"""
        // drop cloumn
        sql "alter table ${table_name} drop column v2"
        qt_sql_10_2 """desc ${table_name}"""
        // add column
        sql "alter table ${table_name} add column v3 variant default null"
        sql """ insert into ${table_name} values (0, '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}',
                     '{"a": 1123, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}, "zzz" : null, "oooo" : {"akakaka" : null, "xxxx" : {"xxx" : 123}}}')"""
        qt_sql_10_3 """desc ${table_name}"""
        //sql "truncate table ${table_name}"

        // varaint column name: chinese name, unicode 
        table_name = "chinese_table"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 5
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
        sql """ insert into ${table_name} values (0, '{"名字" : "jack", "!@#^&*()": "11111", "金额" : 200, "画像" : {"地址" : "北京", "\\\u4E2C\\\u6587": "unicode"}}')"""
        sql """set describe_extend_variant_column = true"""
        qt_sql_11 """desc ${table_name}"""

        // varaint subcolumn: empty
        table_name = "no_subcolumn_table"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 5
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
        sql """ insert into ${table_name} values (0, '{}')"""
        sql """ insert into ${table_name} values (0, '100')"""
        sql """set describe_extend_variant_column = true"""
        qt_sql_12 """desc ${table_name}"""
    } finally {
        // reset flags
        set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "0.95")
    }
}
