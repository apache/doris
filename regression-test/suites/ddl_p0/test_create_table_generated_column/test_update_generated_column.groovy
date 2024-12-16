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

suite("test_generated_column_update") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    // duplicate table
    sql "drop table if exists test_gen_col_update2"
    sql """create table test_gen_col_update2(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""
    sql "insert into test_gen_col_update2 values(1,default,5,default);"
    sql "insert into test_gen_col_update2(a,b) values(4,5);"

    sql "drop table if exists test_gen_col_update2"
    sql """CREATE TABLE `test_gen_col_update2` (
    `id` INT NULL,
    `c1` DECIMAL(10, 4) NULL,
    `c2` TEXT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`, `c1`)
    DISTRIBUTED BY RANDOM BUCKETS 10
    PROPERTIES ("replication_num" = "1");"""
    sql "insert into test_gen_col_update2 values(1,2,'d'),(2,3,'ds'),(3,4,'aa')"

    test {
        sql "update test_gen_col_update2 set a=9,b=10"
        exception "Only unique table could be updated."
    }

    // unique
    sql "drop table if exists test_gen_col_update_unique"
    sql """create table test_gen_col_update_unique(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    unique key(a)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""
    sql "insert into test_gen_col_update_unique values(3,default,5,default);"
    sql "insert into test_gen_col_update_unique(a,b) values(4,5);"
    qt_update_gen_col "update test_gen_col_update_unique set b=10"
    qt_update_select "select * from test_gen_col_update_unique order by a,b,c,d"
    test {
        sql "update test_gen_col_update_unique set c=100"
        exception "The value specified for generated column 'c' in table 'test_gen_col_update_unique' is not allowed."
    }
    test {
        sql "update test_gen_col_update_unique set c=(select 10)"
        exception "The value specified for generated column 'c' in table 'test_gen_col_update_unique' is not allowed."
    }

    qt_update_use_expr "update test_gen_col_update_unique set b=a+100 WHERE a=3;"
    qt_update_use_expr_select "select * from test_gen_col_update_unique order by a,b,c,d"

    qt_update_use_other_table_col "update test_gen_col_update_unique t1 set t1.b=test_gen_col_update2.id from test_gen_col_update2 where t1.a=test_gen_col_update2.id;"
    qt_update_use_expr_select "select * from test_gen_col_update_unique order by a,b,c,d"

    test{
        sql "update test_gen_col_update_unique t1 set t1.c=test_gen_col_update2.id from test_gen_col_update2 where t1.a=test_gen_col_update2.id;"
        exception "The value specified for generated column 'c' in table 'test_gen_col_update_unique' is not allowed."
    }
}