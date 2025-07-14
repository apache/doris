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
    
    check_fold_consistency "regexp_replace('abc123', '123', 'xyz')"
    check_fold_consistency "regexp_replace(null, 'abc', 'def')"
    check_fold_consistency "regexp_replace('abc123', null, 'xyz')"
    check_fold_consistency "regexp_replace('abc123', '123', null)"

    sql """DROP TABLE IF EXISTS `test_table_for_regexp`;"""
    sql """CREATE TABLE test_table_for_regexp (id INT, name VARCHAR(100)) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_table_for_regexp VALUES
        (1, 'abc123'),
        (2, '测试字符串456'),
        (3, 'Phone: 987-654-3210'),
        (4, '这是一个测试'),
        (5, null);"""

    qt_replace_in_table_chinese """SELECT id, regexp_replace(name, '\\\\p{Han}', '汉') as replaced_name FROM test_table_for_regexp;"""

    qt_replace_ignore1 """select regexp_replace('{"abc":5},{"def":78}', '\\}\\,\\{', '\\}&&\\{', 'IGNORE_INVALID_ESCAPE');"""
    qt_replace_ignore2 """select regexp_replace('abc', 'b', "\\}", 'IGNORE_INVALID_ESCAPE');"""
    qt_replace_ignore3 """select regexp_replace_one('{"abc":5},{"def":78}', '\\}\\,\\{', '\\}&&\\{', 'IGNORE_INVALID_ESCAPE');"""
    qt_replace_ignore4 """select regexp_replace_one('abc', 'b', '\\}', 'IGNORE_INVALID_ESCAPE');"""
    qt_replace_ignore5 """select regexp_replace(name, 'b', '\\}', 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_ignore6 """select regexp_replace_one(name, 'b', '\\}', 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""

    qt_replace_const """select regexp_replace_one('abc', name, '\\}', 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace_one('abc', 'b', name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace_one(name, name, '\\}', 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace_one(name, 'b', name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace_one('abc', name, name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace_one(name, name, name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""

    qt_replace_const """select regexp_replace('abc', name, '\\}', 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace('abc', 'b', name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace(name, name, '\\}', 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace(name, 'b', name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace('abc', name, name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""
    qt_replace_const """select regexp_replace_one(name, name, name, 'IGNORE_INVALID_ESCAPE') from test_table_for_regexp order by id;"""

    test {
        sql """
            select regexp_replace('{"abc":5},{"def":78}', '\\}\\,\\{', '\\}&&\\{', 123);
        """
        exception "must be a string type"
    }
    test {
        sql """
            select regexp_replace_one('{"abc":5},{"def":78}', '\\}\\,\\{', '\\}&&\\{', 123);
        """
        exception "must be a string type"
    }
}