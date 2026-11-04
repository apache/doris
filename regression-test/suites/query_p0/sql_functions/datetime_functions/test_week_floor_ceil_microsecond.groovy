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

suite("test_week_floor_ceil_microsecond") {
    sql """DROP TABLE IF EXISTS test_week_floor_ceil_microsecond"""
    sql """
        CREATE TABLE test_week_floor_ceil_microsecond (
            id INT,
            input_value DATETIME(6),
            origin_value DATETIME(6)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_week_floor_ceil_microsecond VALUES
            (1, '2024-01-08 00:00:00.400000', '2024-01-01 00:00:00.500000'),
            (2, '2024-01-08 00:00:00.500000', '2024-01-01 00:00:00.500000'),
            (3, '2024-01-08 00:00:00.600000', '2024-01-01 00:00:00.500000')
    """

    qt_literal_boundary """
        SELECT /*+ SET_VAR(debug_skip_fold_constant = true) */
               week_floor(CAST('2024-01-08 00:00:00.400000' AS DATETIME(6)),
                          1,
                          CAST('2024-01-01 00:00:00.500000' AS DATETIME(6))),
               week_ceil(CAST('2024-01-08 00:00:00.600000' AS DATETIME(6)),
                         1,
                         CAST('2024-01-01 00:00:00.500000' AS DATETIME(6)))
    """

    order_qt_column_boundaries """
        SELECT id,
               input_value,
               week_floor(input_value, 1, origin_value),
               week_ceil(input_value, 1, origin_value)
        FROM test_week_floor_ceil_microsecond
        ORDER BY id
    """
}
