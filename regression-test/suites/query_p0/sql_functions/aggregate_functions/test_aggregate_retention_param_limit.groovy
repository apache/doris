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

// retention() stores its state in a fixed-size events[32] array on BE. Passing more than
// 32 conditions used to overflow that array and core the BE. FE must reject
// > 32 params with a clear error instead of sending the query to BE.
suite("test_aggregate_retention_param_limit") {
    sql "DROP TABLE IF EXISTS retention_param_limit_test"
    sql """
        CREATE TABLE retention_param_limit_test (
            uid INT,
            dt DATETIME
        )
        DUPLICATE KEY(uid)
        DISTRIBUTED BY HASH(uid) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ INSERT INTO retention_param_limit_test VALUES (1, '2026-01-01'), (1, '2026-01-02'), (2, '2026-01-01') """

    def conds = { int n -> (1..n).collect { "uid = ${it}" }.join(", ") }

    // 32 conditions is the maximum allowed and must succeed.
    sql """ SELECT uid, retention(${conds(32)}) FROM retention_param_limit_test GROUP BY uid ORDER BY uid """

    // 33 conditions must be rejected by FE with a clear error (not a BE crash).
    test {
        sql """ SELECT uid, retention(${conds(33)}) FROM retention_param_limit_test GROUP BY uid """
        exception "at most 32"
    }
}
