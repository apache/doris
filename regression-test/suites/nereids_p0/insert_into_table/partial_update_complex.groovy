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

suite("nereids_partial_update_native_insert_stmt_complex", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            sql "set enable_nereids_dml=true;"
            sql "set experimental_enable_nereids_planner=true;"
            sql "set enable_fallback_to_original_planner=false;"
            sql "sync;"

            // test complex partial update
            def tbName1 = "nereids_partial_update_native_insert_stmt_complex1"
            def tbName2 = "nereids_partial_update_native_insert_stmt_complex2"
            def tbName3 = "nereids_partial_update_native_insert_stmt_complex3"

            sql "DROP TABLE IF EXISTS ${tbName1}"
            sql "DROP TABLE IF EXISTS ${tbName2}"
            sql "DROP TABLE IF EXISTS ${tbName3}"

            sql """create table ${tbName1} (
                    id int, 
                    c1 bigint, 
                    c2 string, 
                    c3 double, 
                    c4 date) unique key (id) distributed by hash(id) 
                properties('replication_num'='1', 'enable_unique_key_merge_on_write' = 'true',"store_row_column" = "${use_row_store}"); """

            sql """create table ${tbName2} (
                    id int, 
                    c1 bigint, 
                    c2 string, 
                    c3 double, 
                    c4 date) unique key (id) distributed by hash(id) 
                properties('replication_num'='1', 'enable_unique_key_merge_on_write' = 'true',"store_row_column" = "${use_row_store}"); """

            sql """create table ${tbName3} (id int) distributed by hash (id) properties('replication_num'='1');"""

            sql "set enable_unique_key_partial_update=false;"
            sql "sync;"
            sql """insert into ${tbName1} values
                (1, 1, '1', 1.0, '2000-01-01'),
                (2, 2, '2', 2.0, '2000-01-02'),
                (3, 3, '3', 3.0, '2000-01-03');"""
            sql """insert into ${tbName2} values
                (1, 10, '10', 10.0, '2000-01-10'),
                (2, 20, '20', 20.0, '2000-01-20'),
                (3, 30, '30', 30.0, '2000-01-30'),
                (4, 4, '4', 4.0, '2000-01-04'),
                (5, 5, '5', 5.0, '2000-01-05');"""
            sql """insert into ${tbName3} values(1), (3), (5);"""

            qt_tbl1 "select * from ${tbName1} order by id;"
            qt_tbl2 "select * from ${tbName2} order by id;"
            qt_tbl3 "select * from ${tbName3} order by id;"

            qt_select_result """select ${tbName2}.id, ${tbName2}.c1, ${tbName2}.c3 * 100
                from ${tbName2} inner join ${tbName3} on ${tbName2}.id = ${tbName3}.id order by ${tbName2}.id;"""

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            sql """insert into ${tbName1}(id, c1, c3) 
                select ${tbName2}.id, ${tbName2}.c1, ${tbName2}.c3 * 100
                from ${tbName2} inner join ${tbName3} on ${tbName2}.id = ${tbName3}.id; """

            qt_complex_update """select * from ${tbName1} order by id;"""
            test {
                sql """insert into ${tbName1}
                select ${tbName2}.id, ${tbName2}.c1, ${tbName2}.c3 * 100
                from ${tbName2} inner join ${tbName3} on ${tbName2}.id = ${tbName3}.id; """
                exception "insert into cols should be corresponding to the query output"
            }
            sql "truncate table ${tbName1};"
            sql "truncate table ${tbName2};"
            sql "truncate table ${tbName3};"

            sql "set enable_unique_key_partial_update=false;"
            sql "sync;"
            sql """insert into ${tbName1} values
                (1, 1, '1', 1.0, '2000-01-01'),
                (2, 2, '2', 2.0, '2000-01-02'),
                (3, 3, '3', 3.0, '2000-01-03');"""
            sql """insert into ${tbName2} values
                (1, 10, '10', 10.0, '2000-01-10'),
                (2, 20, '20', 20.0, '2000-01-20'),
                (3, 30, '30', 30.0, '2000-01-30'),
                (4, 4, '4', 4.0, '2000-01-04'),
                (5, 5, '5', 5.0, '2000-01-05');"""
            sql """insert into ${tbName3} values(1), (3), (5);"""

            qt_select_result "select ${tbName2}.id,1 from ${tbName2} inner join ${tbName3} on ${tbName2}.id = ${tbName3}.id order by ${tbName2}.id;"

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            sql """ insert into ${tbName1}(id, __DORIS_DELETE_SIGN__)
                    select ${tbName2}.id,1 from ${tbName2} inner join ${tbName3} on ${tbName2}.id = ${tbName3}.id;"""

            qt_complex_delete """select * from ${tbName1} order by id;"""

            sql "DROP TABLE IF EXISTS ${tbName1}"
            sql "DROP TABLE IF EXISTS ${tbName2}"
            sql "DROP TABLE IF EXISTS ${tbName3}"

            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict = false;"
            sql "set enable_fallback_to_original_planner=true;"
            sql "sync;"
        }
    }
}
