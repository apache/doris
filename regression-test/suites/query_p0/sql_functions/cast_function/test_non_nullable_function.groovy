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

suite("test_non_nullable_function", "query") {
    def tableName = "tbl_test_non_nullable_function"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NULL COMMENT "",
              `k3` ARRAY<VARCHAR(11)> NULL COMMENT "",
              `k4` ARRAY<decimal(27,9)> NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, [1, 2, 3], ["a", "b", "c"], [1.3, 2.14]) """
    sql """ INSERT INTO ${tableName} VALUES(2, [], [], [1.3, 2.14]) """
    sql """ INSERT INTO ${tableName} VALUES(3, [1, 2, 3], [], [1.3, 2.14]) """
    sql """ INSERT INTO ${tableName} VALUES(4, [], ["a", "b", "c"], [1.3, 2.14]) """
    sql """ INSERT INTO ${tableName} VALUES(null, null, null, [1.1,2.2,3.3]) """
    
    qt_nullable "SELECT k1, non_nullable(k1) FROM ${tableName} ORDER BY k1"
    qt_nullable "SELECT k1, non_nullable(k2) FROM ${tableName} ORDER BY k1"
    qt_nullable "SELECT k1, non_nullable(k3) FROM ${tableName} ORDER BY k1"
    try {
        def result = "SELECT k1, non_nullable(k4) FROM ${tableName} ORDER BY k1"
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Try to use originally non-nullable column"))
    }
}
