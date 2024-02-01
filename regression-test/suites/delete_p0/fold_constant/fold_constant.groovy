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

suite("fold_constant") {
    
    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 date null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """
    sql "insert into d_table values(1,curdate());"
    sql "insert into d_table values(2,'2020-01-01');"
    sql "delete from d_table where k2=curdate();"
    qt_select "select * from d_table order by 1;"

    sql "insert into d_table values(4,'2020-01-01');"
    qt_select "select * from d_table order by 1;"
    sql "delete from d_table where k1=3+1;"
    qt_select "select * from d_table order by 1;"

    sql """ DROP TABLE IF EXISTS d_table2; """

    sql """
            create table d_table2(
                k1 int null,
                k2 date null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1",
            "disable_auto_compaction" = "true");
        """
    sql "insert into d_table2 values(1,curdate());"
    sql "insert into d_table2 values(2,'2020-01-01');"
    sql "delete from d_table2 where k2=curdate();"
    qt_select "select * from d_table2 order by 1;"

    sql "insert into d_table2 values(4,'2020-01-01');"
    qt_select "select * from d_table2 order by 1;"
    sql "delete from d_table2 where k1=3+1;"
    qt_select "select * from d_table2 order by 1;"
    qt_select "select 10.0/0, 0.0/10"

    sql """ set enable_fold_constant_by_be = true; """
    sql """ set experimental_enable_nereids_planner = false; """
    qt_select_coalesce_bool "select coalesce(true);"
    qt_select_coalesce_tinyint "select coalesce(cast(1 as tinyint));"
    qt_select_coalesce_smallint "select coalesce(cast(1 as smallint))"
    qt_select_coalesce_int "select coalesce(cast(1 as int));"
    qt_select_coalesce_bigint "select coalesce(cast(1 as bigint));"
    qt_select_coalesce_largeint "select coalesce(cast(1 as largeint));"
    qt_select_coalesce_float "select coalesce(cast(1 as float));"
    qt_select_coalesce_double "select coalesce(cast(1 as double));"
    qt_select_coalesce_datetime "select coalesce(cast(\"2022:10:17 11:16:50\" as datetime));"
    qt_select_coalesce_date "select coalesce(cast(\"2022:10:17 11:16:50\" as date));"
    qt_select_coalesce_decimal "select coalesce(cast(1.123 as decimal));"
    qt_select_coalesce_decimalv2 "select coalesce(cast(1.123456789 as decimal));"
    qt_select_abs1 "select abs(-3);"
    qt_select_abs2 "select abs(-3.2);"
    qt_select_array "select array(true);"
}
