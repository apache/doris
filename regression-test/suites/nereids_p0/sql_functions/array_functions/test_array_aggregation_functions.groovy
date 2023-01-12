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

suite("test_array_aggregation_functions") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "tbl_test_array_aggregation_functions"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `a1` array<tinyint(4)> NULL COMMENT "",
              `a2` array<smallint(6)> NULL COMMENT "",
              `a3` array<int(11)> NULL COMMENT "",
              `a4` array<bigint(20)> NULL COMMENT "",
              `a5` array<largeint(40)> NULL COMMENT "",
              `a6` array<decimal(27, 7)> NULL COMMENT "",
              `a7` array<float> NULL COMMENT "",
              `a8` array<double> NULL COMMENT "",
              `a9` array<date> NULL COMMENT "",
              `a10` array<datetime> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, [1, 2, 3], [100, 101], [1000, 1001], [2147483648, 2147483649], [9223372036854775808], [0.0, 9.999999], [1.0, 1.5], [100.0001, 100.0005], ['2022-10-15', '2022-08-31', '2022-09-01'], ['2022-10-15 10:30:00', '2022-08-31 12:00:00', '2022-09-01 09:00:00']) """
    sql """ INSERT INTO ${tableName} VALUES(2, [1, 2, 3, NULL, 3, 2, 1], NULL, NULL, NULL, NULL, NULL, [127, 128.1], [NULL, 4.023], NULL, NULL) """
    sql """ INSERT INTO ${tableName} VALUES(3, [-1, 0, 1], [-32767, -32768], [-50000, -2147483647], [-9223372036854775808, 0], [-117341182548128045443221445, 170141183460469231731687303715884105727], [-9.999999, 9.999999], [-1.0, 0.0, 1.0], [-128.0001, 127.0001], NULL, NULL) """
    sql """ INSERT INTO ${tableName} VALUES(4, [], [], [], [], [], [], [], [], [], NULL) """

    // Nereids does't support array function
    // qt_select "SELECT k1, array_min(a1), array_min(a2), array_min(a3), array_min(a4), array_min(a5), array_min(a6), array_min(a7), array_min(a8), array_min(a9), array_min(a10) from ${tableName} order by k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, array_max(a1), array_max(a2), array_max(a3), array_max(a4), array_max(a5), array_max(a6), array_max(a7), array_max(a8), array_max(a9), array_max(a10) from ${tableName} order by k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, array_avg(a1), array_avg(a2), array_avg(a3), array_avg(a4), array_avg(a5), array_avg(a6), array_avg(a7), array_avg(a8) from ${tableName} order by k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, array_sum(a1), array_sum(a2), array_sum(a3), array_sum(a4), array_sum(a5), array_sum(a6), array_sum(a7), array_sum(a8) from ${tableName} order by k1"
    // Nereids does't support array function
    // qt_select "SELECT k1, array_product(a1), array_product(a2), array_product(a3), array_product(a4), array_product(a5), array_product(a6), array_product(a7), array_product(a8) from ${tableName} order by k1"
}
