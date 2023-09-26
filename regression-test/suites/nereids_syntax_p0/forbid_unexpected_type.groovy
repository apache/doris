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

suite("forbid_unexpected_type") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """drop table if exists forbid_unexpected_types"""

    sql """
        CREATE TABLE `forbid_unexpected_types` (
          `c_bigint` bigint(20) NULL,
          `c_double` double NULL,
          `c_boolean` boolean NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c_bigint`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        );
    """

    // filter could not have agg function
    test {
        sql """
            select * from forbid_unexpected_types where bitmap_union_count(bitmap_empty()) is NULL;
        """
        exception "LOGICAL_FILTER can not contains AggregateFunction"
    }

    // window function's partition by could not have bitmap
    test {
        sql """
            select min(version()) over (partition by bitmap_union(bitmap_empty())) as c0 from forbid_unexpected_types
        """
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column"
    }

    // window function's order by could not have bitmap
    test {
        sql """
            select min(version()) over (partition by 1 order by bitmap_union(bitmap_empty())) as c0 from forbid_unexpected_types
        """
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column"
    }
}

