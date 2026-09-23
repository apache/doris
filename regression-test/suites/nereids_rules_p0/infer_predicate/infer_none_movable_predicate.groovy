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

suite("infer_none_movable_predicate", "p0") {
    sql "DROP TABLE IF EXISTS infer_none_movable_l"
    sql "DROP TABLE IF EXISTS infer_none_movable_r"
    sql """
        CREATE TABLE infer_none_movable_l (a INT NOT NULL)
        DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE infer_none_movable_r (b INT NOT NULL)
        DUPLICATE KEY(b) DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO infer_none_movable_l VALUES (1), (11)"
    sql "INSERT INTO infer_none_movable_r VALUES (-1), (1), (11)"
    // The unmatched negative row must never evaluate the left-side assertion.
    order_qt_assert_in_or """
        SELECT l.a, r.b
        FROM (SELECT a FROM infer_none_movable_l
              WHERE assert_true(a > 0, 'bad') OR a > 10) l
        JOIN infer_none_movable_r r ON l.a = r.b
    """
}
