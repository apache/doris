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

suite("test_aggregate_finalize_lifecycle", "p0") {
    sql "DROP TABLE IF EXISTS test_aggregate_finalize_lifecycle"
    sql """
        CREATE TABLE test_aggregate_finalize_lifecycle (
            k INT NOT NULL,
            v1 VARCHAR(64) REPLACE,
            v2 VARCHAR(64) REPLACE
        ) AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_aggregate_finalize_lifecycle VALUES
            (1, 'alpha', 'one'), (1, 'alpha', 'one'),
            (2, 'beta', 'two'), (2, 'beta', 'two')
    """
    order_qt_finalized "SELECT k, v1, v2 FROM test_aggregate_finalize_lifecycle ORDER BY k"
}
