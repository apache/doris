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

suite("test_array_functions_of_has_all") {

    def tableName = "tbl_test_has_all"
    // array functions only supported in vectorized engine
    sql """ set enable_vectorized_engine = true """

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT "",
              `k3` ARRAY<int(11)> NULL COMMENT "",
              `k4` ARRAY<double> NULL COMMENT "",
              `k5` ARRAY<double> NULL COMMENT "",
              `k6` ARRAY<CHAR(5)> NULL COMMENT "",
              `k7` ARRAY<CHAR(5)> NULL COMMENT "",
              `k8` ARRAY<date> NULL COMMENT "",
              `k9` ARRAY<date> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1,[1,2,3],[1,2],[1.32,3.45,NULL],[3.45],["hi"],["hi","abc"],["2015-03-13","2015-03-14"],["2015-03-13"]) """
    sql """ INSERT INTO ${tableName} VALUES(2,[1,2,3],[4,5],[1.0],[3.45],["hi"],[],["2015-03-13","2015-03-14"],["2015-03-15"]) """
    sql """ INSERT INTO ${tableName} VALUES(3,[1,2,3],[1,NULL],[1.0,2.0],[],["hi"],[NULL],["2015-03-13"],["2015-03-13","2015-03-13"]) """
    sql """ INSERT INTO ${tableName} VALUES(4,[1,2,3],[NULL],[1.32,3.45,NULL],[1.32,NULL],["hi",NULL],[NULL,"hi"],[],[]) """
    sql """ INSERT INTO ${tableName} VALUES(5,[1,2,3],[],[1.32,3.45,NULL],[NULL],[],[],NULL,[]) """
    sql """ INSERT INTO ${tableName} VALUES(6,[1,2,3],NULL,[1.32,3.45,NULL],NULL,NULL,NULL,NULL,NULL) """
 
    qt_select "SELECT k1, has_all(k2,k3) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, has_all(k4,k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, has_all(k6,k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, has_all(k8,k9) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, has_all(k4,k3) from ${tableName} ORDER BY k1"
   
}
