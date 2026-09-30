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

suite("condition_cache_eligibility") {
    sql "DROP TABLE IF EXISTS condition_cache_eligibility"
    sql """
        CREATE TABLE condition_cache_eligibility (
            k BIGINT,
            a ARRAY<INT>,
            dt1 DATETIME,
            dt2 DATETIME
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO condition_cache_eligibility
        SELECT number, [1,2,3,4,5,6,7], '2026-09-30 12:00:00', '2026-09-30 12:00:00'
        FROM numbers("number" = "100000")
    """

    // Keep seeded random sequences stable across scans; isolate condition cache from result caches.
    sql "set enable_parallel_scan = false"
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    sql "set enable_condition_cache = false"
    order_qt_rand_baseline """
        SELECT count(*) FROM condition_cache_eligibility WHERE k < 0 OR rand(1) < 0.0001
    """
    order_qt_shuffle_baseline """
        SELECT count(*) FROM condition_cache_eligibility WHERE array_shuffle(a, 1) = [1,2,3,4,5,6,7]
    """

    sql "set enable_condition_cache = true"
    order_qt_rand_first """
        SELECT count(*) FROM condition_cache_eligibility WHERE k < 0 OR rand(1) < 0.0001
    """
    order_qt_rand_second """
        SELECT count(*) FROM condition_cache_eligibility WHERE k < 0 OR rand(1) < 0.0001
    """
    order_qt_shuffle_first """
        SELECT count(*) FROM condition_cache_eligibility WHERE array_shuffle(a, 1) = [1,2,3,4,5,6,7]
    """
    order_qt_shuffle_second """
        SELECT count(*) FROM condition_cache_eligibility WHERE array_shuffle(a, 1) = [1,2,3,4,5,6,7]
    """
    order_qt_shuffle_alias """
        SELECT count(*) FROM condition_cache_eligibility WHERE shuffle(a, 1) = [1,2,3,4,5,6,7]
    """
    order_qt_time_date """
        SELECT count(*) FROM condition_cache_eligibility
        WHERE CAST(TIMEDIFF(dt1, dt2) AS DATE) = CURRENT_DATE()
    """
    order_qt_time_date_second """
        SELECT count(*) FROM condition_cache_eligibility
        WHERE CAST(TIMEDIFF(dt1, dt2) AS DATE) = CURRENT_DATE()
    """
    order_qt_deterministic_first """
        SELECT count(*) FROM condition_cache_eligibility WHERE k % 10000 = 0
    """
    order_qt_deterministic_second """
        SELECT count(*) FROM condition_cache_eligibility WHERE k % 10000 = 0
    """
}
