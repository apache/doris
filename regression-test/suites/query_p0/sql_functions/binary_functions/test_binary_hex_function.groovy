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

suite("test_binary_hex_function") {
    qt_sql_1 "select to_binary(NULL);"
    qt_sql_2 "select to_binary('');"
    qt_sql_3 "select to_binary('a');"
    qt_sql_4 "select to_binary('Z');"
    qt_sql_5 "select to_binary('abcdxy');"
    qt_sql_6 "select to_binary('abcdz');"
    qt_sql_7 "select to_binary('abc');"
    qt_sql_8 "select to_binary('ab');"
    qt_sql_9 "select to_binary('abcd');"
    qt_sql_10 "select to_binary('aaabbbcccdddeeefff');"


    qt_sql_11 "select from_binary(NULL);"
    qt_sql_12 "select from_binary(X'');"
    qt_sql_13 "select from_binary(X'61');"
    qt_sql_14 "select from_binary(X'5A');"
    qt_sql_15 "select from_binary(X'616263647879');"
    qt_sql_16 "select from_binary(X'616263647A');"
    qt_sql_17 "select from_binary(X'616263');"
    qt_sql_18 "select from_binary(X'6162');"
    qt_sql_19 "select from_binary(X'61626364');"
    qt_sql_20 "select from_binary(X'616161626262636363646464656565666666');"

    sql " drop table if exists test_binary_hex_function; "
    sql " create table test_binary_hex_function(k1 int, k2 varchar(32)) distributed by hash(k1) buckets 1 properties('replication_num' = '1'); "
    sql " insert into test_binary_hex_function values(1, ''); "
    sql " insert into test_binary_hex_function values(2, 'a'); "
    sql " insert into test_binary_hex_function values(3, 'Z'); "
    sql " insert into test_binary_hex_function values(4, 'abcdxy'); "
    sql " insert into test_binary_hex_function values(5, 'abcdz'); "
    sql " insert into test_binary_hex_function values(6, 'abc'); "
    sql " insert into test_binary_hex_function values(7, 'ab'); "
    sql " insert into test_binary_hex_function values(8, 'abcd'); "
    sql " insert into test_binary_hex_function values(9, 'aaabbbcccdddeeefff'); "
    qt_sql_21 "select k1, k2, to_binary(k2) from test_binary_hex_function order by k1;"
    qt_sql_22 "select k1, k2, from_binary(to_binary(k2)) from test_binary_hex_function order by k1;"
}

