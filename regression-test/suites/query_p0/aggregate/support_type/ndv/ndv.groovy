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

suite("ndv") {
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
        col_float float null,
        col_double double null,
        col_string string null,
        col_date date null,
        col_datetime datetime null,
        col_decimal32 decimal(9,2) null,
        col_decimal64 decimal(18,4) null,
        col_decimal128 decimal(38,8) null,
        col_decimal256 decimal(76,8) null,
        col_ipv4 ipv4 null,
        col_ipv6 ipv6 null
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
         3.14, 2.718281828,
         'string1',
         '2023-01-01', '2023-01-01 12:00:00',
         123.45, 12345.6789, 123456789.12345678, 123456789123456789123456789123456789123456789123456789123456789.12345678,
         '192.168.1.1', '2001:db8::1')
    """
    
    qt_sql_tinyint """select approx_count_distinct(col_tinyint) from d_table;"""
    qt_sql_smallint """select approx_count_distinct(col_smallint) from d_table;"""
    qt_sql_int """select approx_count_distinct(col_int) from d_table;"""
    qt_sql_bigint """select approx_count_distinct(col_bigint) from d_table;"""
    qt_sql_largeint """select approx_count_distinct(col_largeint) from d_table;"""
    qt_sql_float """select approx_count_distinct(col_float) from d_table;"""
    qt_sql_double """select approx_count_distinct(col_double) from d_table;"""

    qt_sql_string """select approx_count_distinct(col_string) from d_table;"""

    qt_sql_date """select approx_count_distinct(col_date) from d_table;"""
    qt_sql_datetime """select approx_count_distinct(col_datetime) from d_table;"""

    qt_sql_decimal32 """select approx_count_distinct(col_decimal32) from d_table;"""
    qt_sql_decimal64 """select approx_count_distinct(col_decimal64) from d_table;"""
    qt_sql_decimal128 """select approx_count_distinct(col_decimal128) from d_table;"""
    qt_sql_decimal256 """select approx_count_distinct(col_decimal256) from d_table;"""

    qt_sql_ipv4 """select approx_count_distinct(col_ipv4) from d_table;"""
    qt_sql_ipv6 """select approx_count_distinct(col_ipv6) from d_table;"""
}