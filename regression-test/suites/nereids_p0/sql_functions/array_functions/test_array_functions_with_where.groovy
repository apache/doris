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

suite("test_array_functions_with_where") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "tbl_test_array_functions_with_where"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT "",
              `k3` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k4` ARRAY<int(11)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, [1, 2, 3], ["a", "b", ""], [1, 2]) """
    sql """ INSERT INTO ${tableName} VALUES(2, [4], NULL, [5]) """
    sql """ INSERT INTO ${tableName} VALUES(3, [], [], NULL) """
    sql """ INSERT INTO ${tableName} VALUES(NULL, [], NULL, NULL) """

    // Nereids does't support array function
    // qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE size(k2)=3 ORDER BY k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE array_contains(k2, 4) ORDER BY k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE element_at(k2, 1)=1 ORDER BY k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE arrays_overlap(k2, k4) ORDER BY k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE cardinality(k2)>0 ORDER BY k1, size(k2)"
    // Nereids does't support array function
    // qt_select "SELECT k1, array_with_constant(5, k1) FROM ${tableName} WHERE k1 is null ORDER BY k1, size(k2)"
    // Nereids does't support array function
    // qt_select "SELECT k1, array(5, k1) FROM ${tableName} WHERE k1 is not null ORDER BY k1, size(k2)"
    // Nereids does't support array function
    // qt_select "SELECT k1, array(k1, 'abc') FROM ${tableName} WHERE k1 is not null ORDER BY k1, size(k2)"
    // Nereids does't support array function
    // qt_select "SELECT k1, array(null, k1) FROM ${tableName} WHERE k1 is not null ORDER BY k1, size(k2)"

    // Nereids does't support array function
    // test {
    //     sql "select k1, size(k2) FROM ${tableName} WHERE k2 = []"
    //     // check exception message contains
    //     exception "Array type dose not support operand"
    // }
}
