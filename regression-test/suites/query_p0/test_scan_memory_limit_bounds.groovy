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

suite("test_scan_memory_limit_bounds") {
    // One column, row, bucket and replica are sufficient to start a nonempty scan.
    // The large value is only a budget; the test does not allocate a large buffer.
    sql "DROP TABLE IF EXISTS test_scan_memory_limit_bounds"
    sql """CREATE TABLE test_scan_memory_limit_bounds (k INT)
           DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO test_scan_memory_limit_bounds VALUES (1)"

    // A single adaptive scan receives the full int64 budget. Its initial 64 MiB estimate
    // yields 137438953471 scanners, which must be capped before conversion to int.
    sql "SET enable_adaptive_scan = true"
    sql "SET exec_mem_limit = 9223372036854775807"
    sql "SET max_scan_mem_ratio = 1"
    sql "SET parallel_pipeline_task_num = 1"
    qt_full_budget "SELECT k FROM test_scan_memory_limit_bounds ORDER BY k"
}
