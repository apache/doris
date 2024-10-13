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

suite("test_regexp_replace") {
    qt_basic_replace "SELECT regexp_replace('abc123', '123', 'xyz');"

    qt_replace_chinese "SELECT regexp_replace('这是一个测试字符串123', '\\\\p{Han}+', '汉');"
    
    def re_fe
    def re_be
    def re_no_fold

    def check_three_ways = { test_sql ->
        re_fe = order_sql "select/*+SET_VAR(enable_fold_constant_by_be=false)*/ ${test_sql}"
        re_be = order_sql "select/*+SET_VAR(enable_fold_constant_by_be=true)*/ ${test_sql}"
        re_no_fold = order_sql "select/*+SET_VAR(debug_skip_fold_constant=true)*/ ${test_sql}"
        logger.info("check on sql: ${test_sql}")
        qt_check_fe "select/*+SET_VAR(enable_fold_constant_by_be=false)*/ ${test_sql}"
        qt_check_be "select/*+SET_VAR(enable_fold_constant_by_be=true)*/ ${test_sql}"
        qt_check_no_fold "select/*+SET_VAR(debug_skip_fold_constant=true)*/ ${test_sql}"
        assertEquals(re_fe, re_be)
        assertEquals(re_fe, re_no_fold)
    }

    check_three_ways "regexp_replace('abc123', '123', 'xyz')"
    check_three_ways "regexp_replace(null, 'abc', 'def')"
    check_three_ways "regexp_replace('abc123', null, 'xyz')"
    check_three_ways "regexp_replace('abc123', '123', null)"

    sql """DROP TABLE IF EXISTS `test_table_for_regexp`;"""
    sql """CREATE TABLE test_table_for_regexp (id INT, name VARCHAR(100)) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_table_for_regexp VALUES
        (1, 'abc123'),
        (2, '测试字符串456'),
        (3, 'Phone: 987-654-3210'),
        (4, '这是一个测试'),
        (5, null);"""

    qt_replace_in_table_chinese """SELECT id, regexp_replace(name, '\\\\p{Han}', '汉') as replaced_name FROM test_table_for_regexp;"""
}