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

suite("test_width_bucket_function", "arrow_flight_sql") {
    qt_sql "select width_bucket(1, 2, 3, 2)"
    qt_sql "select width_bucket(null, 2, 3, 2)"
    qt_sql "select width_bucket(6, 2, 6, 4)"
    qt_sql "select width_bucket(3, 2, 6, 4)"
    qt_sql "select width_bucket(29000, 20000, 60000, 4)"
    qt_sql "select width_bucket(date('2022-11-18'), date('2022-11-17'), date('2022-11-19'), 2)"
    
    def tableName1 = "tbl_test_width_bucket_function_not_null"
    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
              `k1` int NOT NULL COMMENT "",
              `v1` date NOT NULL COMMENT "",
              `v2` double NOT NULL COMMENT "",
              `v3` bigint NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName1} VALUES (1, "2022-11-18", 290000.00, 290000) """
    sql """ INSERT INTO ${tableName1} VALUES (2, "2023-11-18", 320000.00, 320000) """
    sql """ INSERT INTO ${tableName1} VALUES (3, "2024-11-18", 399999.99, 399999) """
    sql """ INSERT INTO ${tableName1} VALUES (4, "2025-11-18", 400000.00, 400000) """
    sql """ INSERT INTO ${tableName1} VALUES (5, "2026-11-18", 470000.00, 470000) """
    sql """ INSERT INTO ${tableName1} VALUES (6, "2027-11-18", 510000.00, 510000) """
    sql """ INSERT INTO ${tableName1} VALUES (7, "2028-11-18", 610000.00, 610000) """

    qt_select "SELECT * FROM ${tableName1} ORDER BY k1"

    qt_select "SELECT k1, width_bucket(v1, date('2023-11-18'), date('2027-11-18'), 4) FROM ${tableName1} ORDER BY k1"
    qt_select "SELECT k1, width_bucket(v2, 200000, 600000, 4) FROM ${tableName1} ORDER BY k1"
    qt_select "SELECT k1, width_bucket(v3, 200000, 600000, 4) FROM ${tableName1} ORDER BY k1"

    def tableName2 = "tbl_test_width_bucket_function_null"
    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
              `k1` int NULL COMMENT "",
              `v1` date NULL COMMENT "",
              `v2` double NULL COMMENT "",
              `v3` bigint NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName2} VALUES (1, "2022-11-18", 290000.00, 290000) """
    sql """ INSERT INTO ${tableName2} VALUES (2, "2023-11-18", 320000.00, 320000) """
    sql """ INSERT INTO ${tableName2} VALUES (3, "2024-11-18", 399999.99, 399999) """
    sql """ INSERT INTO ${tableName2} VALUES (4, "2025-11-18", 400000.00, 400000) """
    sql """ INSERT INTO ${tableName2} VALUES (5, "2026-11-18", 470000.00, 470000) """
    sql """ INSERT INTO ${tableName2} VALUES (6, "2027-11-18", 510000.00, 510000) """
    sql """ INSERT INTO ${tableName2} VALUES (7, "2028-11-18", 610000.00, 610000) """
    sql """ INSERT INTO ${tableName2} VALUES (8, null, null, null) """

    qt_select "SELECT * FROM ${tableName2} ORDER BY k1"

    qt_select "SELECT k1, width_bucket(v1, date('2023-11-18'), date('2027-11-18'), 4) FROM ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, width_bucket(v2, 200000, 600000, 4) FROM ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, width_bucket(v3, 200000, 600000, 4) FROM ${tableName2} ORDER BY k1"

    qt_select_width_bucket_1 "select width_bucket(10,0,11,10);"
    qt_select_width_bucket_2 "select width_bucket(cast(10 as int),0,11,10);"
    qt_select_width_bucket_3 "select width_bucket(10.0,0,11,10);"
    qt_select_width_bucket_4 "select width_bucket(10,0,10.1,10);"
    qt_select_width_bucket_5 "select width_bucket(10,0,10.10,10);"

    test {
      sql "select width_bucket(4, 0, 8, 0)"
      exception "buckets must be a positive integer value"
    }
}