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

suite("test_join_on", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS join_on"
    sql """
     CREATE TABLE join_on (
      `k1` int(11) NULL,
       d_array ARRAY<int> ,
       hll_col HLL ,
      `k3` bitmap,
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`k1`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into join_on values (1, [1, 2], hll_hash(1), bitmap_from_string('1, 3, 5, 7, 9, 11, 13, 99, 19910811, 20150402')); """
    sql """insert into join_on values (2, [2, 3], hll_hash(2), bitmap_from_string('2, 4, 6, 8, 10, 12, 14, 100, 19910812, 20150403')); """
    qt_sql """ select * from join_on order by k1; """
    test {
        sql """ select * from join_on as j1 inner join join_on as j2 on j1.d_array = j2.d_array; """
        exception "Method get_max_row_byte_size is not supported for Array"
    }
    test {
        sql """ select * from join_on as j1 inner join join_on as j2 on j1.hll_col = j2.hll_col; """
        exception "data type HLL could not used in ComparisonPredicate"
    }

    test {
        sql """ select * from join_on as j1 inner join join_on as j2 on j1.k3 = j2.k3; """
        exception "data type BITMAP could not used in ComparisonPredicate"
    }
}
