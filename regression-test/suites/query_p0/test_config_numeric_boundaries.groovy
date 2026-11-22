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

suite("test_config_numeric_boundaries", "p0,nonConcurrent") {
    // These bounds fail with one config update and no table data. Zero clone timeouts
    // break the scheduler invariant; negative lock waits cannot wait for a busy lock. A fragment
    // timeout must leave room for the 5000 ms RPC grace period before it reaches LONG_MAX.
    [min_clone_task_timeout_sec: ["0"], max_clone_task_timeout_sec: ["0"],
     try_commit_lock_timeout_seconds: ["-1"],
     remote_fragment_exec_timeout_ms: ["9223372036854770808", "9223372036854775807"]].each { name, values ->
        def original = getFeConfig(name)
        try {
            values.each { value ->
                test {
                    sql "ADMIN SET FRONTEND CONFIG ('${name}' = '${value}')"
                    exception "must be"
                }
            }
        } finally {
            // Restore the value even if an unfixed server accepts the invalid update.
            sql "ADMIN SET FRONTEND CONFIG ('${name}' = '${original}')"
        }
    }

    // One row reaches the scanner divisor. A 4096 MiB setting used to wrap to zero;
    // the table does not need to contain 4 GiB of data to reproduce the failure.
    sql "drop table if exists test_config_numeric_boundaries"
    sql """create table test_config_numeric_boundaries (k int)
        duplicate key(k) distributed by hash(k) buckets 1
        properties ('replication_num' = '1')"""
    sql "insert into test_config_numeric_boundaries values (0)"
    // The serial scanner uses doris_scan_range_max_mb to size each scan range.
    try {
        sql "set enable_parallel_scan = false"
        // This is the first second count that exceeds signed int milliseconds.
        // It must remain a valid query timeout through FE planning and BE execution.
        sql "set query_timeout = 2147484"
        setBeConfigTemporary([doris_scan_range_max_mb: 4096]) {
            order_qt_large_scan_range "select k from test_config_numeric_boundaries"
        }
    } finally {
        sql "set enable_parallel_scan = default"
        sql "set query_timeout = default"
    }
}
