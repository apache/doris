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

suite("test_array_functions") {
    def tableName = "tbl_test_array_functions"
    // array functions only supported in vectorized engine
    sql """ set enable_vectorized_engine = true """

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT "",
              `k3` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k4` ARRAY<int(11)> NULL COMMENT "",
              `k5` ARRAY<CHAR(5)> NULL COMMENT "",
              `k6` ARRAY<date> NULL COMMENT "",
              `k7` ARRAY<datetime> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1,[1,2,3],["a","b",""],[1,2],["hi"],["2015-03-13"],["2015-03-13 12:36:38"]) """
    sql """ INSERT INTO ${tableName} VALUES(2,[4],NULL,[5],["hi2"],NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(3,[],[],NULL,["hi3"],NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(4,[1,2,3,4,5,4,3,2,1],[],[],NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(5,[],["a","b","c","d","c","b","a"],NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(6,[1,2,3,4,5,4,3,2,1],["a","b","c","d","c","b","a"],NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(7,[8,9,NULL,10,NULL],["f",NULL,"g",NULL,"h"],NULL,NULL,NULL,NULL) """

    qt_select "SELECT k1, size(k2), size(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, cardinality(k2), cardinality(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_distinct(k2), array_distinct(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k2, k1), k1 FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_sort(k2), array_sort(k3), array_sort(k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k2, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k2, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, reverse(k2), reverse(k3), reverse(k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_join(k2, '_', 'null'), array_join(k3, '-', 'null') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k7) from ${tableName} ORDER BY k1"
}
