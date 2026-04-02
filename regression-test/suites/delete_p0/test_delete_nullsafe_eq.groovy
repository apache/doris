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

suite("test_delete_nullsafe_eq", "delete_p0") {
    sql "DROP TABLE IF EXISTS test_delete_nullsafe_eq"

    sql """
        CREATE TABLE test_delete_nullsafe_eq (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """INSERT INTO test_delete_nullsafe_eq VALUES (1, 10), (2, 20)"""

    try {
        sql """SET delete_without_partition = true"""
        sql """DELETE FROM test_delete_nullsafe_eq WHERE k1 <=> 1"""
        sql """SYNC"""

        order_qt_after_delete """SELECT * FROM test_delete_nullsafe_eq ORDER BY k1"""
    } finally {
        sql """SET delete_without_partition = false"""
    }
}
