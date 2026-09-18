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

suite("test_dup_key_compact_row_position") {
    sql "DROP TABLE IF EXISTS test_dup_key_compact_row_position"
    sql "DROP TABLE IF EXISTS test_dup_key_compact_row_position_src"

    sql """
        CREATE TABLE test_dup_key_compact_row_position_src (
            shard_num SMALLINT NOT NULL,
            label_name VARCHAR(64) NOT NULL,
            label_value ARRAY<VARCHAR(128)> NOT NULL,
            uid INT NOT NULL,
            dt DATE NOT NULL
        )
        DUPLICATE KEY(shard_num, label_name)
        DISTRIBUTED BY HASH(shard_num) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE test_dup_key_compact_row_position (
            shard_num SMALLINT NOT NULL,
            label_name VARCHAR(64) NOT NULL,
            label_value ARRAY<VARCHAR(128)> NOT NULL,
            uid INT NOT NULL,
            dt DATE NOT NULL
        )
        DUPLICATE KEY(shard_num, label_name)
        DISTRIBUTED BY HASH(shard_num) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO test_dup_key_compact_row_position_src VALUES
            (2, 'b', ['first'], 20, '2026-08-15'),
            (1, 'a', ['old'], 10, '2026-08-15'),
            (1, 'z', [], 30, '2026-08-15')
    """
    sql """
        INSERT INTO test_dup_key_compact_row_position_src VALUES
            (1, 'a', ['new', 'value'], 11, '2026-08-15'),
            (0, 'z', ['lowest'], 1, '2026-08-15'),
            (2, 'b', ['latest'], 21, '2026-08-15')
    """

    sql """
        INSERT INTO test_dup_key_compact_row_position
        SELECT shard_num, label_name, label_value, uid, dt
        FROM test_dup_key_compact_row_position_src
    """

    order_qt_dup_key_array """
        SELECT shard_num, label_name, label_value, uid, dt
        FROM test_dup_key_compact_row_position
        ORDER BY shard_num, label_name, uid
    """
}
