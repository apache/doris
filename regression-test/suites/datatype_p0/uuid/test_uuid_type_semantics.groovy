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

suite("test_uuid_type_semantics") {
    sql "DROP TABLE IF EXISTS test_uuid"
    sql """
        CREATE TABLE test_uuid (
            id INT,
            value UUID NOT NULL,
            nullable_value UUID NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO test_uuid VALUES
            (1, '00000000-0000-0000-0000-000000000000', NULL),
            (2, '550E8400-E29B-41D4-A716-446655440000',
                'ffffffffffffffffffffffffffffffff'),
            (3, '00000000000000010000000000000000',
                '550e8400-e29b-41d4-a716-446655440000')
    """

    order_qt_values "SELECT * FROM test_uuid ORDER BY id"
    order_qt_ordering "SELECT id FROM test_uuid ORDER BY value"
    order_qt_predicate """
        SELECT id FROM test_uuid
        WHERE value >= CAST('00000000-0000-0001-0000-000000000000' AS UUID)
        ORDER BY id
    """
    order_qt_aggregation """
        SELECT CAST(MIN(value) AS STRING), CAST(MAX(value) AS STRING), COUNT(DISTINCT value)
        FROM test_uuid
    """
    order_qt_nested """
        SELECT CAST(ARRAY(
            CAST('00000000-0000-0000-0000-000000000001' AS UUID),
            CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID)
        ) AS STRING)
    """
    order_qt_casts """
        SELECT
            CAST(CAST('550E8400E29B41D4A716446655440000' AS UUID) AS STRING),
            CAST('invalid' AS UUID),
            TRY_CAST('{550e8400-e29b-41d4-a716-446655440000}' AS UUID)
    """
    order_qt_functions """
        SELECT
            UUID_VERSION(UUID_V4()),
            UUID_VERSION(UUID_V7()),
            UUID_VERSION(GENERATEUUIDV4()),
            UUID_VERSION(GENERATEUUIDV7()),
            LENGTH(CAST(UUID_V4() AS STRING)),
            LENGTH(CAST(UUID_V7() AS STRING))
    """

    test {
        sql "INSERT INTO test_uuid VALUES (4, 'not-a-uuid', NULL)"
        exception "null value for not null column"
    }
}
