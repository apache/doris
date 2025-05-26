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

suite("explode") {
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode([1,2,3]) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([1,2,3]) tmp1 as e1; """

    // array is null
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode(null) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer(null) tmp1 as e1; """

    // array is empty
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode([]) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([]) tmp1 as e1; """

    // array with null elements
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode([null,1,null]) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([null,1,null]) tmp1 as e1; """

    sql """ DROP TABLE IF EXISTS d_table; """
    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"

    qt_test1 """select e1 from (select k1 from d_table) as t lateral view explode_numbers(5) tmp1 as e1;"""
    qt_test2 """select e1 from (select k1 from d_table) as t lateral view explode_numbers(5) tmp1 as e1 where e1=k1;"""
    qt_test3 """select e1,k1 from (select k1 from d_table) as t lateral view explode_numbers(5) tmp1 as e1;"""

    sql """ DROP TABLE IF EXISTS baseall_explode_numbers; """
    sql """
            CREATE TABLE `baseall_explode_numbers` (
            `k3` int(11) NULL
            ) ENGINE=OLAP
            duplicate KEY(`k3`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k3`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );
        """
    sql "insert into baseall_explode_numbers values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15);"
    qt_test4 """select k3,e from baseall_explode_numbers as U lateral view explode_numbers(5) tmp1 as e order by k3,e;"""
    qt_test5 """select k3,e from baseall_explode_numbers as U lateral view explode_numbers(10) tmp1 as e order by k3,e;"""

    // test array nested array | map for explode
    def testTable = "tam"
    def dataFile = "am.json"
    sql """ DROP TABLE IF EXISTS $testTable; """
    sql """
        CREATE TABLE `$testTable` (
          `id` bigint(20) NULL,
          `arr_arr` array<array<text>> NULL DEFAULT "[]",
          `arr_map` array<MAP<text,text>> NULL DEFAULT "[]"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

     streamLoad {
            table testTable

            // set http request header params
            file dataFile // import json file
            set 'format', 'json' // import format
            set 'read_json_by_line', 'true' // read json by line
            set 'strict_mode', 'true'
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
                assertEquals(25, json.NumberTotalRows)
                assertEquals(25, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
            }
     }

     sql "sync"

     // check result
     order_qt_sql """ select id, eaa from $testTable lateral view explode(arr_arr) aa as eaa order by id; """
     order_qt_sql """ select id, eam from $testTable lateral view explode(arr_map) aa as eam order by id; """

     def res_origin_am = sql "select array_size(arr_map) from $testTable where array_size(arr_map) > 0 order by id;"
     def res_explode_am = sql "select count() from (select id, eam from $testTable lateral view explode(arr_map) aa as eam order by id) as t1  group by id order by id;"
    for (int r = 0; r < res_origin_am.size(); ++ r) {
        assertEquals(res_origin_am[r][0], res_explode_am[r][0])
    }

     def res_origin_size_aa = sql "select array_size(arr_arr) from $testTable where array_size(arr_arr) > 0 order by id;"
     def res_explode_aa = sql "select count() from (select id, eaa from $testTable lateral view explode(arr_arr) aa as eaa order by id) as t1  group by id order by id;"
    for (int r = 0; r < res_origin_size_aa.size(); ++ r) {
        assertEquals(res_origin_size_aa[r][0], res_explode_aa[r][0])
    }

    sql "DROP TABLE IF EXISTS array_test;"
    sql """
    CREATE TABLE `array_test` (
      `id` INT NULL,
      `array_int` ARRAY<INT> NOT NULL,
      `array_string` ARRAY<String> NULL,
      `v_int` VARIANT NULL,
      `v_string` VARIANT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "is_being_synced" = "false",
    "storage_medium" = "hdd",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000",
    "group_commit_data_bytes" = "134217728"
    );
    """

    sql """insert into array_test values( 1, [4,5,6], ["2","3"], '{"a": [4,5,6]}', '{"a": ["2","3"]}'),( 2, [14,15], ["2",null], '{"a": [14,15]}', '{"a": ["2",null]}'),( 3, [114,115,116], null, '{"a": [114,115,116]}','{"a": null}');"""


    qt_test6 "select id,e1 from array_test as a lateral view explode(a.array_string) tmp1 as e1;"
    qt_test7 "select id,e1 from array_test as a lateral view explode(a.array_int) tmp1 as e1;"
    qt_test8 "select id,e1,e2 from array_test as a lateral view explode(a.array_int,a.array_string) tmp1 as e1,e2;"
    qt_test9 "select id,e1,e2 from array_test as a lateral view explode(a.array_string,a.array_int) tmp1 as e1,e2;"
    qt_test10 "select id,e1,e2,e3 from array_test as a lateral view explode(a.array_string,a.array_int,a.array_int) tmp1 as e1,e2,e3;"
    qt_test11 "select id,e1,e2,e11,e12 from array_test as a lateral view explode(a.array_int,a.array_string) tmp1 as e1,e2 lateral view explode(a.array_int,a.array_string) tmp2 as e11,e12;"

    qt_test12 "select id,e1 from array_test as a lateral view explode_outer(a.array_string) tmp1 as e1;"
    qt_test13 "select id,e1 from array_test as a lateral view explode_outer(a.array_int) tmp1 as e1;"
    qt_test14 "select id,e1,e2 from array_test as a lateral view explode_outer(a.array_int,a.array_string) tmp1 as e1,e2;"
    qt_test15 "select id,e1,e2 from array_test as a lateral view explode_outer(a.array_string,a.array_int) tmp1 as e1,e2;"
    qt_test16 "select id,e1,e2,e3 from array_test as a lateral view explode_outer(a.array_string,a.array_int,a.array_int) tmp1 as e1,e2,e3;"
    qt_test17 "select id,e1,e2,e11,e12 from array_test as a lateral view explode_outer(a.array_int,a.array_string) tmp1 as e1,e2 lateral view explode_outer(a.array_int,a.array_string) tmp2 as e11,e12;"

    qt_test18 "select id,e1 from array_test as a lateral view explode_variant_array(a.v_string['a']) tmp1 as e1;"
    qt_test19 "select id,e1 from array_test as a lateral view explode_variant_array(a.v_int['a']) tmp1 as e1;"
    qt_test20 "select id,e1,e2 from array_test as a lateral view explode_variant_array(a.v_int['a'],a.v_string['a']) tmp1 as e1,e2;"
    qt_test21 "select id,e1,e2 from array_test as a lateral view explode_variant_array(a.v_string['a'],a.v_int['a']) tmp1 as e1,e2;"
    qt_test22 "select id,e1,e2,e3 from array_test as a lateral view explode_variant_array(a.v_string['a'],a.v_int['a'],a.v_int['a']) tmp1 as e1,e2,e3;"
    qt_test23 "select id,e1,e2,e11,e12 from array_test as a lateral view explode_variant_array(a.v_int['a'],a.v_string['a']) tmp1 as e1,e2 lateral view explode_variant_array(a.v_int['a'],a.v_string['a']) tmp2 as e11,e12;"

}
