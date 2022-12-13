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

suite("test_issue_13606") {
    def tableName1 = "test_issue_13606_1"
    def tableName2 = "test_issue_13606_2"
    // array functions only supported in vectorized engine
    sql """ set enable_vectorized_engine = true """

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
            CREATE TABLE IF NOT EXISTS ${tableName2} (
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

    sql """ INSERT INTO ${tableName1} VALUES(1, [1,2,3]),(3,[3,2,1]),(3,[3,2,1,NULL]),(2,[3,4,5]) """
    sql """ INSERT INTO ${tableName2} VALUES(1,[2]),(2,[3]) """

    qt_select "SELECT ${tableName1}.k1 as t1k1,array_min(${tableName2}.a1) as min FROM ${tableName1} JOIN ${tableName2} WHERE array_max(${tableName1}.a1) = array_max(${tableName2}.a1) ORDER BY t1k1,min"
    qt_select "SELECT array_min(${tableName2}.a1) as min FROM ${tableName1} JOIN ${tableName2} WHERE array_max(${tableName1}.a1) = array_max(${tableName2}.a1) ORDER BY min"
}
