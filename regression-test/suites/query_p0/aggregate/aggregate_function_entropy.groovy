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

suite("test_entropy") {

    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """set enable_decimal256=true"""

    qt_entropy_null """select entropy(NULL)"""
    qt_entropy_literal """select entropy(1)"""

    sql """drop table if exists test_entropy_base"""
    sql """
    create table test_entropy_base (
        k1 int null,
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
        col_ipv6 ipv6 null,
        col_array array<int> null,
        col_map map<string, int> null,
        col_struct struct<name:string, age:int> null,
        col_bitmap bitmap not null,
        col_hll hll not null,
        col_quantile_state quantile_state not null
        )
        duplicate key (k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1")
    """

    sql """
        insert into test_entropy_base VALUES
        (0, null,
        null, null, null, null, null,
        null, null,
        null, null, null,
        null, null,
        null, null, null, null,
        null, null,
        null, null, null,
        to_bitmap(0), hll_hash(0), to_quantile_state(0,2048)
        ),

        (1, true,
        10, 1000, 1000000, 10000000000, 100000000000000000000,
        3.14, 1.718281828,
        'char1', 'varchar1', 'string1',
        '2023-01-01', '2023-01-01 12:00:00',
        123.45, 12345.6789, 123456789.12345678, 123456789123456789123456789123456789123456789123456789123456789.12345678,
        '192.168.1.1', '2001:db8::1',
        [1,2,3], {'key1':100, 'key2':200}, named_struct('name','John1','age',21),
        to_bitmap(1), hll_hash(1), to_quantile_state(1,2048)
        ),

        (2, true,
        20, 2000, 2000000, 20000000000, 200000000000000000000,
        3.24, 2.718281828,
        'char2', 'varchar2', 'string2',
        '2024-01-02', '2024-01-02 12:00:00',
        223.45, 22345.6789, 223456789.12345678, 223456789123456789123456789123456789123456789123456789123456789.12345678,
        '192.168.1.2', '2001:db8::2',
        [4,5,6], {'key1':200, 'key2':400}, named_struct('name','John2','age',22),
        to_bitmap(2), hll_hash(2), to_quantile_state(2,2048)
        ),

        (3, true,
        30, 3000, 3000000, 30000000000, 300000000000000000000,
        3.34, 3.718281828,
        'char3', 'varchar3', 'string3',
        '2025-01-03', '2025-01-03 12:00:00',
        323.45, 32345.6789, 323456789.12345678, 323456789123456789123456789123456789123456789123456789123456789.12345678,
        '192.168.1.3', '2001:db8::3',
        [7,8,9], {'key1':300, 'key2':600}, named_struct('name','John3','age',23),
        to_bitmap(3), hll_hash(3), to_quantile_state(3,2048)
        ),

        (4, true,
        40, 4000, 4000000, 40000000000, 400000000000000000000,
        3.44, 4.718281828,
        'char4', 'varchar4', 'string4',
        '2026-01-04', '2026-01-04 12:00:00',
        423.45, 42345.6789, 423456789.12345678, 423456789123456789123456789123456789123456789123456789123456789.12345678,
        '192.168.1.4', '2001:db8::4',
        [10,11,12], {'key1':400, 'key2':800}, named_struct('name','John4','age',24),
        to_bitmap(4), hll_hash(4), to_quantile_state(4,2048)
        ),

        (5, true,
        50, 5000, 5000000, 50000000000, 500000000000000000000,
        3.54, 5.718281828,
        'char5', 'varchar5', 'string5',
        '2027-01-05', '2027-01-05 12:00:00',
        523.45, 52345.6789, 523456789.12345678, 523456789123456789123456789123456789123456789123456789123456789.12345678,
        '192.168.1.5', '2001:db8::5',
        [13,14,15], {'key1':500, 'key2':1000}, named_struct('name','John5','age',25),
        to_bitmap(5), hll_hash(5), to_quantile_state(5,2048)
        )"""
    
    sql """drop table if exists test_entropy"""
    sql """
        create table test_entropy 
        duplicate key (k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1") 
        as
            select test_entropy_base.* from test_entropy_base
            lateral view explode(
                array(
                    1,2,2,2,5,0,
                    1,4,4,3,3,0,
                    5,5,3,5,1,0,
                    1,1,2,3,4,0,
                    5,3,4,4,2,0
                )
            ) test_entropy_seq as k
            where test_entropy_seq.k = test_entropy_base.k1
    """

    qt_entropy_boolean """select entropy(col_boolean) from test_entropy"""
    qt_entropy_tinyint """select entropy(col_tinyint) from test_entropy"""
    qt_entropy_smallint """select entropy(col_smallint) from test_entropy"""
    qt_entropy_int """select entropy(col_int) from test_entropy"""
    qt_entropy_bigint """select entropy(col_bigint) from test_entropy"""
    qt_entropy_largeint """select entropy(col_largeint) from test_entropy"""
    qt_entropy_float """select entropy(col_float) from test_entropy"""
    qt_entropy_double """select entropy(col_double) from test_entropy"""

    qt_entropy_char """select entropy(col_char) from test_entropy"""
    qt_entropy_varchar """select entropy(col_varchar) from test_entropy"""
    qt_entropy_string """select entropy(col_string) from test_entropy"""

    qt_entropy_date """select entropy(col_date) from test_entropy"""
    qt_entropy_datetime """select entropy(col_datetime) from test_entropy"""

    qt_entropy_decimal32 """select entropy(col_decimal32) from test_entropy"""
    qt_entropy_decimal64 """select entropy(col_decimal64) from test_entropy"""
    qt_entropy_decimal128 """select entropy(col_decimal128) from test_entropy"""
    qt_entropy_decimal256 """select entropy(col_decimal256) from test_entropy"""

    qt_entropy_ipv4 """select entropy(col_ipv4) from test_entropy"""
    qt_entropy_ipv6 """select entropy(col_ipv6) from test_entropy"""

    qt_entropy_array """select entropy(col_array) from test_entropy"""
    qt_entropy_map """select entropy(col_map) from test_entropy"""
    qt_entropy_struct """select entropy(col_struct) from test_entropy"""

    qt_entropy_multicol """select entropy(col_int, col_string, col_datetime) from test_entropy"""

    qt_entropy_groupby """select entropy(col_int) from test_entropy group by k1 % 2 order by k1%2"""
    qt_entropy_window """select entropy(col_int) over (partition by k1%2) from test_entropy order by k1%2"""

    qt_entropy_empty """select entropy(col_int) from test_entropy where 1=0"""

}
