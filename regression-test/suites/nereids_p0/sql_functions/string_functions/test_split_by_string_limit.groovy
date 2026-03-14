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

suite("test_split_by_string_limit") {
    // === Constant expression tests with limit ===

    // Basic limit functionality
    qt_limit1 "select split_by_string('one,two,three,', ',', 2);"
    qt_limit2 "select split_by_string('one,two,three,', ',', 3);"
    qt_limit3 "select split_by_string('one,two,three,', ',', 4);"
    qt_limit4 "select split_by_string('one,two,three,', ',', 10);"
    qt_limit5 "select split_by_string('one,two,three', ',', 1);"

    // limit = -1 (no limit, same as 2-arg)
    qt_limit6 "select split_by_string('one,two,three,', ',', -1);"

    // limit = 0 (no limit, same as 2-arg)
    qt_limit7 "select split_by_string('a,b,c', ',', 0);"

    // Empty source string + limit
    qt_limit8 "select split_by_string('', ',', 2);"

    // Empty delimiter + limit (split by character)
    qt_limit9 "select split_by_string('abcde', '', 3);"
    qt_limit10 "select split_by_string('abcde', '', 1);"
    qt_limit11 "select split_by_string('abcde', '', 10);"

    // Multi-char delimiter + limit
    qt_limit12 "select split_by_string('a::b::c::d', '::', 2);"
    qt_limit13 "select split_by_string('a::b::c::d', '::', 3);"
    qt_limit14 "select split_by_string('1,,2,3,,4,5,,abcde', ',,', 2);"

    // NULL handling
    qt_limit15 "select split_by_string(NULL, ',', 2);"
    qt_limit_null2 "select split_by_string('a,b,c', NULL, 2);"

    // UTF-8 + limit
    qt_limit16 "select split_by_string('你a好b世c界', '', 3);"
    qt_limit_utf8_delim "select split_by_string('上海北北京北杭州', '北', 2);"
    qt_limit_utf8_empty "select split_by_string('你好世界', '', 2);"

    // Edge cases: consecutive delimiters + limit
    qt_limit17 "select split_by_string(',,,', ',', 2);"
    qt_limit18 "select split_by_string(',,a,b,c,', ',', 3);"

    // Trailing delimiter + limit
    qt_limit_trail1 "select split_by_string('a,b,c,', ',', 2);"
    qt_limit_trail2 "select split_by_string('a,b,c,', ',', 4);"
    qt_limit_trail3 "select split_by_string('a,b,c,', ',', 5);"

    // Delimiter equals source
    qt_limit_eq "select split_by_string(',', ',', 2);"

    // Single character source
    qt_limit_single "select split_by_string('a', ',', 2);"

    // Regex special characters as delimiter + limit
    qt_limit_regex1 "select split_by_string('a..b..c', '..', 2);"
    qt_limit_regex2 "select split_by_string('a||b||c', '||', 2);"
    qt_limit_regex3 "select split_by_string('a((b((c', '((', 2);"

    // cast type
    qt_limit_cast1 "select split_by_string(cast('a,b,c' as string), cast(',' as string), 2);"

    // Delimiter not found in source
    qt_limit_nomatch "select split_by_string('hello world', 'xyz', 2);"

    // Non-CJK Unicode + limit
    qt_limit_unicode "select split_by_string('ṭṛì ḍḍumai ṭṛì', ' ', 2);"

    // Case-insensitive function name
    qt_limit_upper "select SPLIT_BY_STRING('a,b,c,d', ',', 2);"

    // Nested in other functions: array_size
    qt_limit_nested1 "select array_size(split_by_string('a,b,c,d', ',', 2));"
    qt_limit_nested2 "select array_size(split_by_string('a,b,c,d', ',', 10));"

    // Nested: array_map with limit result
    qt_limit_arraymap """select array_map(x->upper(x), split_by_string('one,two,three', ',', 2))"""

    // Used in WHERE clause
    qt_limit_where "select 1 where array_size(split_by_string('a,b,c', ',', 2)) = 2;"

    // === Table data tests with multiple column types ===
    sql """DROP TABLE IF EXISTS test_split_limit"""
    sql """
        CREATE TABLE IF NOT EXISTS test_split_limit (
            `k1` int(11) NULL COMMENT "",
            `v1` varchar(50) NULL COMMENT "",
            `v2` varchar(10) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
    """
    sql """ INSERT INTO test_split_limit VALUES(1, 'a,b,c,d', ',') """
    sql """ INSERT INTO test_split_limit VALUES(2, 'x::y::z', '::') """
    sql """ INSERT INTO test_split_limit VALUES(3, 'hello', ',') """
    sql """ INSERT INTO test_split_limit VALUES(4, null, ',') """
    sql """ INSERT INTO test_split_limit VALUES(5, 'a,b,c,d,e', ',') """
    sql """ INSERT INTO test_split_limit VALUES(6, ',,,', ',') """
    sql """ INSERT INTO test_split_limit VALUES(7, '', ',') """
    sql """ INSERT INTO test_split_limit VALUES(8, 'a,b,c,', ',') """

    // column, column, literal
    qt_table1 "SELECT k1, split_by_string(v1, v2, 2) FROM test_split_limit ORDER BY k1"
    qt_table2 "SELECT k1, split_by_string(v1, v2, 3) FROM test_split_limit ORDER BY k1"

    // column, literal, literal
    qt_table3 "SELECT k1, split_by_string(v1, ',', 2) FROM test_split_limit ORDER BY k1"
    qt_table6 "SELECT k1, split_by_string(v1, ',', 1) FROM test_split_limit ORDER BY k1"

    // literal, column, literal
    qt_table4 "SELECT k1, split_by_string('a,b,c,d,e', v2, 2) FROM test_split_limit ORDER BY k1"

    // literal, literal, literal (all constants — tests use_default_implementation_for_constants path)
    qt_table5 "SELECT split_by_string('a,b,c,d', ',', 2)"

    // Test with different string column types
    sql """DROP TABLE IF EXISTS test_split_limit_types"""
    sql """
        CREATE TABLE IF NOT EXISTS test_split_limit_types (
            `rid` INT NULL,
            `str` TEXT NULL,
            `vc`  VARCHAR(50) NULL,
            `chr` CHAR(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`rid`)
        DISTRIBUTED BY HASH(`rid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
    """
    sql """ INSERT INTO test_split_limit_types VALUES(1, 'a,b,c,d', 'a,b,c,d', 'a,b,c,d') """
    sql """ INSERT INTO test_split_limit_types VALUES(2, 'x::y::z', 'x::y::z', 'x::y::z') """
    sql """ INSERT INTO test_split_limit_types VALUES(3, null, null, null) """

    // TEXT type + limit
    qt_type1 "SELECT rid, split_by_string(str, ',', 2) FROM test_split_limit_types ORDER BY rid"
    // VARCHAR type + limit
    qt_type2 "SELECT rid, split_by_string(vc, ',', 2) FROM test_split_limit_types ORDER BY rid"
    // CHAR type + limit
    qt_type3 "SELECT rid, split_by_string(chr, ',', 2) FROM test_split_limit_types ORDER BY rid"

    // 2-arg with table data still works (no regression)
    qt_table_2arg "SELECT k1, split_by_string(v1, v2) FROM test_split_limit ORDER BY k1"

    // split alias + limit with table data
    qt_table_alias "SELECT k1, split(v1, v2, 2) FROM test_split_limit ORDER BY k1"

    // === split alias + limit ===
    qt_alias1 "select split('one,two,three', ',', 2);"
    qt_alias2 "select split('a::b::c', '::', 2);"
    qt_alias3 "select split('a,b,c,d', ',', 3);"

    // === Verify 2-arg still works (no regression) ===
    qt_noregress1 "select split_by_string('a,b,c', ',');"
    qt_noregress2 "select split_by_string('abcde', '');"
    qt_noregress3 "select split_by_string('你a好b世c界', '');"

    // === Negative tests: limit must be a constant integer ===
    test {
        sql "SELECT k1, split_by_string(v1, ',', k1) FROM test_split_limit"
        exception "must be a constant integer"
    }
    test {
        sql "select split_by_string('a,b,c', ',', 'abc')"
        exception "must be a constant integer"
    }

    // === testFoldConst: verify FE constant folding matches BE execution ===
    testFoldConst("select split_by_string('a,b,c,d', ',', 2)")
    testFoldConst("select split_by_string('a,b,c,d', ',', 3)")
    testFoldConst("select split_by_string('a,b,c,d', ',', 10)")
    testFoldConst("select split_by_string('a,b,c,d', ',', 1)")
    testFoldConst("select split_by_string('a,b,c,d', ',', -1)")
    testFoldConst("select split_by_string('a,b,c,d', ',', 0)")
    testFoldConst("select split_by_string('abcde', '', 3)")
    testFoldConst("select split_by_string('a::b::c', '::', 2)")
    testFoldConst("select split_by_string('', ',', 2)")
    testFoldConst("select split_by_string(',,,', ',', 2)")
    testFoldConst("select split_by_string('你a好b世c界', '', 3)")
    testFoldConst("select split_by_string('上海北北京北杭州', '北', 2)")
    testFoldConst("select split_by_string('a,b,c,', ',', 2)")
    testFoldConst("select split_by_string('a,b,c,', ',', 5)")
    testFoldConst("select split_by_string('a..b..c', '..', 2)")
    testFoldConst("select split_by_string(',', ',', 2)")
    testFoldConst("select split_by_string('a', ',', 2)")
    testFoldConst("select split_by_string('hello world', 'xyz', 2)")
    testFoldConst("select split_by_string('ṭṛì ḍḍumai ṭṛì', ' ', 2)")
    testFoldConst("select split_by_string('a||b||c', '||', 2)")
    testFoldConst("select split_by_string('a,b,c,', ',', 4)")
    testFoldConst("select split_by_string(',,a,b,c,', ',', 3)")
}
