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

suite("nereids_lateral_view") {
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"

    sql """DROP TABLE IF EXISTS nlv_test"""

    sql """
        CREATE TABLE `nlv_test` (
            `c1` int NULL,
            `c2` varchar(100) NULL,
            `c3` varchar(100) NULL,
            `c4` varchar(100) NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """INSERT INTO nlv_test VALUES(1, '["abc", "def"]', '[1,2]', '[1.1,2.2]')"""
    sql """INSERT INTO nlv_test VALUES(2, 'valid', '[1,2]', '[1.1,2.2]')"""
    sql """INSERT INTO nlv_test VALUES(3, '["abc", "def"]', 'valid', '[1.1,2.2]')"""
    sql """INSERT INTO nlv_test VALUES(4, '["abc", "def"]', '[1,2]', 'valid')"""


    qt_all_function_inner """
        SELECT * FROM nlv_test
          LATERAL VIEW explode_numbers(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double(c4) lv4 AS clv4
    """

    qt_all_function_outer """
        SELECT * FROM nlv_test
          LATERAL VIEW explode_numbers_outer(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string_outer(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int_outer(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double_outer(c4) lv4 AS clv4
    """

    qt_column_prune """
        SELECT clv1, clv3, c2, c4 FROM nlv_test
          LATERAL VIEW explode_numbers(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string_outer(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double_outer(c4) lv4 AS clv4
    """

    qt_alias_query """
        SELECT clv1, clv3, c2, c4 FROM (SELECT * FROM nlv_test) tmp
          LATERAL VIEW explode_numbers(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string_outer(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double_outer(c4) lv4 AS clv4
    """
}
