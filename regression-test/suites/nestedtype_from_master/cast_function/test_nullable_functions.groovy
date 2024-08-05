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

suite("test_nullable_functions", "query") {
    def tableName = "test_nullable_functions"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NULL COMMENT "",
              `k3` ARRAY<VARCHAR(11)> NULL COMMENT "",
              `k4` ARRAY<decimal(27,9)> NOT NULL COMMENT "",
              `k5` int(11) NOT NULL,
              `k6` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, [1, 2, 3], ["a", "b", "c"], [1.3, 2.14], 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES(2, [], [], [1.3, 2.14], 2, 2) """
    sql """ INSERT INTO ${tableName} VALUES(3, [1, 2, 3], [], [1.3, 2.14], 3, 3) """
    sql """ INSERT INTO ${tableName} VALUES(4, [], ["a", "b", "c"], [1.3, 2.14], 4, null) """

    qt_nullable_1 "SELECT k1, non_nullable(k1), nullable(k1) FROM ${tableName} ORDER BY k1"
    qt_nullable_2 "SELECT k1, non_nullable(k2), nullable(k4) FROM ${tableName} ORDER BY k1"
    qt_nullable_3 "SELECT k1, non_nullable(k3), nullable(k5) FROM ${tableName} ORDER BY k1"
    test {
        sql "SELECT k1, non_nullable(k4) FROM ${tableName} ORDER BY k1"
        exception "Try to use originally non-nullable column"
    }
    test {
        sql "SELECT k1, non_nullable(k6) FROM ${tableName} ORDER BY k1"
        exception "There's NULL value"
    }
    qt_ignore "SELECT ignore(k1*k5, k2, k3, k4, k5) FROM ${tableName} ORDER BY k1"
}
