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

suite("test_array_functions_of_array_difference") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_array_functions_of_array_difference"
    // open enable_array_type
    sql "ADMIN SET FRONTEND CONFIG ('enable_array_type' = 'true')"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, []) """
    sql """ INSERT INTO ${tableName} VALUES(2, [NULL]) """
    sql """ INSERT INTO ${tableName} VALUES(3, [1,NULL,3]) """
    sql """ INSERT INTO ${tableName} VALUES(4, [1,2,3]) """
    sql """ INSERT INTO ${tableName} VALUES(5, [16,7,8]) """
    sql """ INSERT INTO ${tableName} VALUES(6, [1,2,3,4,5,4,3,2,1]) """
    sql """ INSERT INTO ${tableName} VALUES(7, [1111,12324,8674,123,3434,435,45,53,54,2]) """


    // Nereids does't support array function
    // qt_select "SELECT *, array_difference(k2) FROM ${tableName}"

}
