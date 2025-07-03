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

suite("test_array_contains_all") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tableName1 = "test_array_contains_all"
    def tableName2 = "test_array_contains_all_2"
    sql """DROP TABLE IF EXISTS ${tableName1}"""
    sql """DROP TABLE IF EXISTS ${tableName2}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
              `k1` int(11) NULL COMMENT "",
              `a1` ARRAY<int(11)> NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """
        CREATE TABLE ${tableName2} (
        id bigint not null ,
        array_col1   Array<BigInt>,
        array_col2   Array<BigInt>
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """ INSERT INTO ${tableName1} VALUES(1, [1,2,3]),(3,[3,2,1]),(3,[3,2,1,NULL]),(2,[3,4,5]) """
    sql """ INSERT INTO ${tableName1} VALUES(1,[2]),(2,[3]) """
    sql """
      insert into ${tableName2} values
          (1,[null,1],[null]),
          (2,[null],[1,3,4,5,1,2]),
          (3,[],[]),
          (4,null,[]),
          (5,[4,4,4],[4,null]),
          (6,[1,1,2,1,1,2,3,3],[1,2,3]);
    """

    qt_select1 "select array_contains_all([1,2,3,4], [1,2,3]);"
    qt_select2 "select array_contains_all([1,2,3,4], [3,2]);"
    qt_select3 "select array_contains_all([1, 2, NULL, 3, 4], ['a']);"
    qt_select4 "select array_contains_all([1,2,3,4,null], null);"
    qt_select5 "select array_contains_all([1,2,3,4], []);"
    qt_select6 "select array_contains_all([1,2,3,4], [2,3]);"
    qt_select7 "select array_contains_all([1,2,3,4], [3,2]);"
    qt_select8 "select array_contains_all([1,2,3,4], [1,2,3]);"
    qt_select9 "select array_contains_all([1,2,3,4], [1,2,4]);"
    qt_select10 "select array_contains_all([], []);"
    qt_select11 "select array_contains_all([1,null], [null]);"
    qt_select12 "select array_contains_all([1.0,2,3,4], [1]);"
    qt_select13 "select array_contains_all(array(cast(1.0 as decimal),2,3,4), array(cast(1 as int)));"
    qt_select14 "select array_contains_all(['a','b','c'], ['a','b']);"
    qt_select15 "select array_contains_all(['a','b','c'], ['a','c']);"
    // cast a as NULL, return true"
    qt_select17 "select array_contains_all([1, 2, NULL, 3, 4], ['a']);"
    qt_select18 "select array_contains_all([1, 2, NULL, 3, 4], [2,3]);"
    qt_select19 "select array_contains_all([1, 2, NULL, 3, 4], null);"
    qt_select20 "select array_contains_all(null, [2,3]);"
    qt_select21 "select array_contains_all([1, 2, NULL, 3, 4], [null,null]);"
    qt_select22 "select array_contains_all([1, 2, NULL], [null,2]);"
    qt_select23 "select array_contains_all(null, null);"
    qt_select24 "select array_contains_all([1, 1, 2, NULL], [1,2]);"
    qt_select25 "select array_contains_all(array_col1,array_col2) from test_array_contains_all_2 order by id;"
    qt_select26 "select array_contains_all(array_col1,array_col1) from test_array_contains_all_2 order by id;"
    qt_select27 "select array_contains_all(array_col2,array_col1) from test_array_contains_all_2 order by id;"
    qt_select27 "select array_contains_all(array_col2,null) from test_array_contains_all_2 order by id;"

    sql """DROP TABLE IF EXISTS test_array_string_decimal"""
    sql """
            CREATE TABLE IF NOT EXISTS test_array_string_decimal (
              `k1` int(11) NULL COMMENT "",
              `str` ARRAY<string> NULL COMMENT "",
              `decimal_col` ARRAY<decimal(10,3)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
    """
    sql """
      insert into test_array_string_decimal values
          (1,[null],[null]),
          (2,['a','b','c'],[1.22,1.33,1.44]),
          (3,['aa','bb','cc','dd'],[0.22,0.33,0.44]),
          (4,null,[]),
          (5,['asdasd'],[1233434.234]),
          (6,[],[9999.099,32434.123]);
    """
    qt_select28 "select array_contains_all([],str) from test_array_string_decimal order by k1;"
    qt_select29 "select array_contains_all(['a'],str), str from test_array_string_decimal order by k1;"
    qt_select30 "select array_contains_all(['a','b','c'],str) from test_array_string_decimal order by k1;"
    qt_select31 "select array_contains_all(['a','b','c','d'],str) from test_array_string_decimal order by k1;"
    qt_select32 "select array_contains_all(['q,','a','b','c','d'],str) from test_array_string_decimal order by k1;"
    qt_select33 "select array_contains_all(['q,','a','asdasd','c','d'],str) from test_array_string_decimal order by k1;"
    qt_select34 "select array_contains_all([1233434.234],decimal_col) from test_array_string_decimal order by k1;"
    qt_select35 "select array_contains_all([1.22,1.33,1.44,12.3,123.2],decimal_col) from test_array_string_decimal order by k1;"
    qt_select36 "select array_contains_all([1.22,1.33,1.44,9999.099,32434.123,12.3,123.2],decimal_col),decimal_col from test_array_string_decimal order by k1;"
    qt_select37 "select array_contains_all(null,decimal_col) from test_array_string_decimal order by k1;"

}
