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
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

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

    // UTF-8 + limit
    qt_limit16 "select split_by_string('你a好b世c界', '', 3);"

    // Edge cases: consecutive delimiters + limit
    qt_limit17 "select split_by_string(',,,', ',', 2);"
    qt_limit18 "select split_by_string(',,a,b,c,', ',', 3);"

    // === Table data tests ===
    def tableName = "test_split_limit"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
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
    sql """ INSERT INTO ${tableName} VALUES(1, 'a,b,c,d', ',') """
    sql """ INSERT INTO ${tableName} VALUES(2, 'x::y::z', '::') """
    sql """ INSERT INTO ${tableName} VALUES(3, 'hello', ',') """
    sql """ INSERT INTO ${tableName} VALUES(4, null, ',') """
    sql """ INSERT INTO ${tableName} VALUES(5, 'a,b,c,d,e', ',') """

    qt_table1 "SELECT k1, split_by_string(v1, v2, 2) FROM ${tableName} ORDER BY k1"
    qt_table2 "SELECT k1, split_by_string(v1, v2, 3) FROM ${tableName} ORDER BY k1"

    // === split alias + limit ===
    qt_alias1 "select split('one,two,three', ',', 2);"
    qt_alias2 "select split('a::b::c', '::', 2);"

    // === Verify 2-arg still works (no regression) ===
    qt_noregress1 "select split_by_string('a,b,c', ',');"
    qt_noregress2 "select split_by_string('abcde', '');"
}
