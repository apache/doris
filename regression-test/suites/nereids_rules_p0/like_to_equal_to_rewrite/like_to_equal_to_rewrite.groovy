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

suite("like_to_equal_to_rewrite") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "drop table if exists mal_test_to_equal_to"
    sql """
        create table mal_test_to_equal_to(a varchar(10), b int) 
        distributed by hash(a) buckets 32
        properties(
        "replication_allocation"="tag.location.default: 1"
        );"""

    sql "insert into mal_test_to_equal_to values('abc',1);"
    qt_test_regular "select * from mal_test_to_equal_to where a like 'abc';"

    sql "truncate table mal_test_to_equal_to;"
    sql "insert into mal_test_to_equal_to values('abc%',1);"
    qt_test_const_percent_sign "select * from mal_test_to_equal_to where a like 'abc\\%';"

    sql "truncate table mal_test_to_equal_to;"
    sql "insert into mal_test_to_equal_to values('abc_',1);"
    qt_test_underline """select * from mal_test_to_equal_to where a like 'abc\\_';"""

    sql "truncate table mal_test_to_equal_to;"
    sql "insert into mal_test_to_equal_to values('abc\\\\',1);"
    qt_test_backslash1 "select * from mal_test_to_equal_to where a like 'abc\\\\\\\\';"
    qt_test_backslash2 "select * from mal_test_to_equal_to where a like 'abc\\\\';"

    sql "truncate table mal_test_to_equal_to;"
    sql "insert into mal_test_to_equal_to values('abc\\\\%',1);"
    qt_test_backslash_percent_sign_odd "select * from mal_test_to_equal_to where a like 'abc\\\\\\\\\\%';"

    sql "truncate table mal_test_to_equal_to;"
    sql "insert into mal_test_to_equal_to values('abc\\\\\\%',1);"
    qt_test_backslash_percent_sign_even_match "select * from mal_test_to_equal_to where a like 'abc\\\\\\\\\\\\\\\\\\\\%';"
    qt_test_backslash_percent_sign_even_cannot_match "select * from mal_test_to_equal_to where a like 'abc\\\\\\\\\\\\\\\\\\\\\\\\%';"
}