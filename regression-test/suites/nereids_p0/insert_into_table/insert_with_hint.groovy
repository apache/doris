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

suite('nereids_insert_with_hint') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    def tableName = "nereids_insert_with_hint"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """insert into ${tableName} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
    qt_1 """ select * from ${tableName} order by id; """
    // partial update using insert stmt in non-strict mode,
    // existing rows should be updated and new rows should be inserted with unmentioned columns filled with default or null value
    sql """insert /*+ SET_VAR(enable_unique_key_partial_update=true, enable_insert_strict = false)*/
        into ${tableName}(id,score) values(2,400),(1,200),(4,400);"""
    qt_1 """ select * from ${tableName} order by id; """
    sql """ DROP TABLE IF EXISTS ${tableName} """


    def tableName2 = "nereids_insert_with_hint2" 
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
            CREATE TABLE ${tableName2} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321",
                `update_time` date NULL)
            UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "update_time"
            )"""
    sql """ insert into ${tableName2} values
            (2, "doris2", 2000, 223, 1, '2023-01-01'),
            (1, "doris", 1000, 123, 1, '2023-01-01');"""
    qt_2 "select * from ${tableName2} order by id;"
    // partial update with seq col
    sql """ insert /*+ SET_VAR(enable_unique_key_partial_update=true, enable_insert_strict = false)*/
            into ${tableName2}(id,score,update_time) values
            (2,2500,"2023-07-19"),
            (2,2600,"2023-07-20"),
            (1,1300,"2022-07-19"),
            (3,1500,"2022-07-20"),
            (3,2500,"2022-07-18"); """
    qt_2 "select * from ${tableName2} order by id;"
    sql """ DROP TABLE IF EXISTS ${tableName2}; """



    def tableName4 = "nereids_insert_with_hint4"
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """
            CREATE TABLE ${tableName4} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                `score` int(11) NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """insert into ${tableName4} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1),(3,"doris3",5000,34,345);"""
    qt_4 """ select * from ${tableName4} order by id; """
    // partial update with delete sign
    sql """ insert /*+ SET_VAR(enable_unique_key_partial_update=true, enable_insert_strict = false)*/ 
        into ${tableName4}(id,__DORIS_DELETE_SIGN__) values(2,1);"""
    qt_4 """ select * from ${tableName4} order by id; """
    sql """ DROP TABLE IF EXISTS ${tableName4} """



    def tableName6 = "nereids_insert_with_hint6"
    sql """ DROP TABLE IF EXISTS ${tableName6} """
    sql """create table ${tableName6} (
        k int null,
        v int null,
        v2 int null,
        v3 int null
    ) unique key (k) distributed by hash(k) buckets 1
    properties("replication_num" = "1",
    "enable_unique_key_merge_on_write"="true",
    "disable_auto_compaction"="true"); """
    sql "insert into ${tableName6} values(1,1,3,4),(2,2,4,5),(3,3,2,3),(4,4,1,2);"
    qt_6 "select * from ${tableName6} order by k;"
    sql "set enable_unique_key_partial_update=true;"
    sql "sync;"
    sql "insert /*+ SET_VAR(enable_unique_key_partial_update=true) */into ${tableName6}(k,v) select v2,v3 from ${tableName6};"
    qt_6 "select * from ${tableName6} order by k;"
    sql "set enable_unique_key_partial_update=false;"
    sql "set enable_insert_strict = false;"
    sql "sync;"
    sql """ DROP TABLE IF EXISTS ${tableName6}; """
}