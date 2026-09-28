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

suite("test_condition_cache_nondeterministic") {
    sql "DROP TABLE IF EXISTS test_condition_cache_nondeterministic"
    sql """
        CREATE TABLE test_condition_cache_nondeterministic (
            k BIGINT,
            v BIGINT
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_condition_cache_nondeterministic
        SELECT number, number FROM numbers("number" = "100000")
    """

    sql "set enable_condition_cache = true"
    // Each scanner has its own rand(1) sequence, so read the tablet with one scanner.
    sql "set enable_parallel_scan = false"

    // The filter uses column k, so the storage layer runs it. rand(1) gives each row the next
    // number of one sequence, so reusing a cached result would change the second count.
    order_qt_first_run """
        SELECT count(*) FROM test_condition_cache_nondeterministic
        WHERE k < 0 OR rand(1) < 0.0001
    """
    order_qt_second_run """
        SELECT count(*) FROM test_condition_cache_nondeterministic
        WHERE k < 0 OR rand(1) < 0.0001
    """
}
