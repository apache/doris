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

// Explicit rollup duplicate keys must be sortable: FE rejects them up front instead of
// letting the asynchronous rollup job fail in BE.
suite("test_rollup_dup_key_type") {
    sql "DROP TABLE IF EXISTS test_rollup_dup_key_type"
    sql """
        CREATE TABLE test_rollup_dup_key_type (
            id INT,
            v VARIANT,
            a ARRAY<INT>,
            m MAP<INT, INT>,
            st STRUCT<x:INT>,
            j JSON,
            s STRING
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_rollup_dup_key_type SELECT 1, parse_to_variant('{"a": 10}'), [1], map(1, 1),
            named_struct('x', 1), '{"a": 1}', 'x'"""
    sql """INSERT INTO test_rollup_dup_key_type SELECT 2, parse_to_variant('{"a": 20}'), [2], map(2, 2),
            named_struct('x', 2), '{"a": 2}', 'y'"""

    for (String col : ["v", "a", "m", "st", "j"]) {
        test {
            sql "ALTER TABLE test_rollup_dup_key_type ADD ROLLUP r_${col} (${col}, id) DUPLICATE KEY(${col})"
            exception "Column[${col}] can not be used as a duplicate key of rollup"
        }
    }

    // A value column of the same types is still allowed after a sortable key.
    sql "ALTER TABLE test_rollup_dup_key_type ADD ROLLUP r_s (s, id, v) DUPLICATE KEY(s)"
    def state = ""
    for (int i = 0; i < 120; i++) {
        state = sql("SHOW ALTER TABLE ROLLUP WHERE TableName = 'test_rollup_dup_key_type' "
                + "ORDER BY CreateTime DESC LIMIT 1")[0][8]
        if (state == "FINISHED" || state == "CANCELLED") {
            break
        }
        sleep(1000)
    }
    assertEquals("FINISHED", state)
    order_qt_rollup_s "SELECT s, id, CAST(v['a'] AS INT) FROM test_rollup_dup_key_type"

    sql "DROP TABLE IF EXISTS test_rollup_dup_key_type_create"
    test {
        sql """
            CREATE TABLE test_rollup_dup_key_type_create (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            ROLLUP (r_v (v, id) DUPLICATE KEY(v))
            PROPERTIES ("replication_num" = "1")
        """
        exception "Column[v] can not be used as a duplicate key of rollup"
    }
}
