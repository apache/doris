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

suite("min") {
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
        col_boolean boolean null,
        col_tinyint tinyint null,
        col_smallint smallint null,
        col_int int null,
        col_bigint bigint null,
        col_largeint largeint null,
        col_float float null,
        col_double double null,
        col_char char(10) null,
        col_varchar varchar(100) null,
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
         true, 100, 10000, 1000000, 10000000000, 100000000000000000000,
         3.14, 2.718281828,
         'char1', 'varchar1', 'string1',
         '2023-01-01', '2023-01-01 12:00:00',
         123.45, 12345.6789, 123456789.12345678, 123456789123456789123456789123456789123456789123456789123456789.12345678,
         '192.168.1.1', '2001:db8::1')
    """
    
    sql """
    insert into d_table values 
        (2, 2, 2, 'test2', 
         false, 50, 5000, 500000, 5000000000, 50000000000000000000,
         2.71, 3.141592653,
         'char2', 'varchar2', 'string2',
         '2023-02-01', '2023-02-01 13:30:00',
         234.56, 23456.7890, 234567890.23456789, 234567890234567890234567890234567890234567890234567890234567890.23456789,
         '10.0.0.1', '2001:db8::2')
    """
    
    sql """
    insert into d_table values 
        (3, 3, 3, 'test3', 
         true, 75, 7500, 750000, 7500000000, 75000000000000000000,
         1.41, 1.732050808,
         'char3', 'varchar3', 'string3',
         '2023-03-01', '2023-03-01 14:45:00',
         345.67, 34567.8901, 345678901.34567890, 345678901345678901345678901345678901345678901345678901345678901.34567890,
         '172.16.0.1', '2001:db8::3')
    """
    
    // 测试 min 函数对各种数据类型的支持
    qt_min_boolean """select min(col_boolean) from d_table;"""
    qt_min_tinyint """select min(col_tinyint) from d_table;"""
    qt_min_smallint """select min(col_smallint) from d_table;"""
    qt_min_int """select min(col_int) from d_table;"""
    qt_min_bigint """select min(col_bigint) from d_table;"""
    qt_min_largeint """select min(col_largeint) from d_table;"""
    qt_min_float """select min(col_float) from d_table;"""
    qt_min_double """select min(col_double) from d_table;"""

    qt_min_char """select min(col_char) from d_table;"""
    qt_min_varchar """select min(col_varchar) from d_table;"""
    qt_min_string """select min(col_string) from d_table;"""

    qt_min_date """select min(col_date) from d_table;"""
    qt_min_datetime """select min(col_datetime) from d_table;"""

    qt_min_decimal32 """select min(col_decimal32) from d_table;"""
    qt_min_decimal64 """select min(col_decimal64) from d_table;"""
    qt_min_decimal128 """select min(col_decimal128) from d_table;"""
    qt_min_decimal256 """select min(col_decimal256) from d_table;"""

    qt_min_ipv4 """select min(col_ipv4) from d_table;"""
    qt_min_ipv6 """select min(col_ipv6) from d_table;"""
}