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

suite("avg") {
    sql "set enable_decimal256 = true;"
    sql """
        drop table if exists d_table;
    """
    
    sql """
    create table d_table (
        k1 int null,
        k2 int not null,
        k3 bigint null,
        k4 varchar(100) null,
        col_tinyint tinyint null,
        col_smallint smallint null,
        col_int int null,
        col_bigint bigint null,
        col_largeint largeint null,
        col_double double null,
        col_decimal32 decimal(9,2) null,
        col_decimal64 decimal(18,4) null,
        col_decimal128 decimal(38,8) null,
        col_decimal256 decimal(76,8) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    
    // 插入测试数据
    sql """
    insert into d_table values 
        (1, 1, 1, 'test1', 
         100, 10000, 1000000, 10000000000, 100000000000000000000,
         2.718281828,
         123.45, 12345.6789, 123456789.12345678, 123456789123456789123456789123456789123456789123456789123456789.12345678)
    """
    
    qt_sql_tinyint """select avg(col_tinyint) from d_table;"""
    qt_sql_smallint """select avg(col_smallint) from d_table;"""
    qt_sql_int """select avg(col_int) from d_table;"""
    qt_sql_bigint """select avg(col_bigint) from d_table;"""
    qt_sql_largeint """select avg(col_largeint) from d_table;"""
    qt_sql_double """select avg(col_double) from d_table;"""

    qt_sql_decimal32 """select avg(col_decimal32) from d_table;"""
    qt_sql_decimal64 """select avg(col_decimal64) from d_table;"""
    qt_sql_decimal128 """select avg(col_decimal128) from d_table;"""
    qt_sql_decimal256 """select avg(col_decimal256) from d_table;"""
}