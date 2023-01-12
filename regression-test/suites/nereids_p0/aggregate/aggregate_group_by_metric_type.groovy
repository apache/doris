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

suite("aggregate_group_by_metric_type") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def error_msg = "column must use with specific function, and don't support filter or group by"
    sql "DROP TABLE IF EXISTS test_group_by_hll_and_bitmap"

    sql """
        CREATE TABLE IF NOT EXISTS test_group_by_hll_and_bitmap (id int, user_ids bitmap bitmap_union, hll_set hll hll_union) 
        ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """

    sql "insert into test_group_by_hll_and_bitmap values(1, bitmap_hash(1), hll_hash(1))"

    // Nereids does't support array function
    // test {
    //     sql "select distinct user_ids from test_group_by_hll_and_bitmap"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select distinct hll_set from test_group_by_hll_and_bitmap"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select user_ids from test_group_by_hll_and_bitmap order by user_ids"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select hll_set from test_group_by_hll_and_bitmap order by hll_set"
    //     exception "${error_msg}"
    // }


    // Nereids does't support array function
    // test {
    //     sql "select distinct user_ids from test_group_by_hll_and_bitmap"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select distinct hll_set from test_group_by_hll_and_bitmap"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select user_ids from test_group_by_hll_and_bitmap order by user_ids"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select hll_set from test_group_by_hll_and_bitmap order by hll_set"
    //     exception "${error_msg}"
    // }

    sql "DROP TABLE test_group_by_hll_and_bitmap"

    sql "DROP TABLE IF EXISTS test_group_by_array"
    sql """
        CREATE TABLE IF NOT EXISTS test_group_by_array (id int, c_array array<int>) ENGINE=OLAP DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1 properties("replication_num" = "1");
        """
    sql "insert into test_group_by_array values(1, [1,2,3])"

    // Nereids does't support array function
    // test {
    //     sql "select distinct c_array from test_group_by_array"
    //     exception "${error_msg}"
    // }
    // Nereids does't support array function
    // test {
    //     sql "select c_array from test_group_by_array order by c_array"
    //     exception "${error_msg}"
    // }
    // Nereids does't support array function
    // test {
    //     sql "select c_array,count(*) from test_group_by_array group by c_array"
    //     exception "${error_msg}"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select distinct c_array from test_group_by_array"
    //     exception "${error_msg}"
    // }
    // Nereids does't support array function
    // test {
    //     sql "select c_array from test_group_by_array order by c_array"
    //     exception "${error_msg}"
    // }
    // Nereids does't support array function
    // test {
    //     sql "select c_array,count(*) from test_group_by_array group by c_array"
    //     exception "${error_msg}"
    // }

    sql "DROP TABLE test_group_by_array"
}
