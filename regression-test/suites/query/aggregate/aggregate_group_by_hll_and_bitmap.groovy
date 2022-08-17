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

suite("aggregate_group_by_hll_and_bitmap") {
    sql "DROP TABLE IF EXISTS test_group_by_hll_and_bitmap"

    sql """
        CREATE TABLE test_group_by_hll_and_bitmap (id int, user_ids bitmap bitmap_union, hll_set hll hll_union) 
        ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """

    sql "insert into test_group_by_hll_and_bitmap values(1, bitmap_hash(1), hll_hash(1))"

    test {
        sql "select distinct user_ids from test_group_by_hll_and_bitmap"
        exception "Doris hll and bitmap column must use with specific function, and don't support filter or group by.please run 'help hll' or 'help bitmap' in your mysql client"
    }

    test {
        sql "select distinct hll_set from test_group_by_hll_and_bitmap"
        exception "Doris hll and bitmap column must use with specific function, and don't support filter or group by.please run 'help hll' or 'help bitmap' in your mysql client"
    }

    sql "DROP TABLE test_group_by_hll_and_bitmap"
}