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

suite("test_array_range_null_argument") {
    sql "DROP TABLE IF EXISTS test_array_range_null_argument"
    sql """
        CREATE TABLE test_array_range_null_argument (
            id INT,
            s INT NULL,
            e INT NULL,
            st INT NULL,
            ds DATETIME(6) NULL,
            de DATETIME(6) NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // The rows are read in one block. A row with a NULL argument gives NULL, and must not build a
    // range from the value under the NULL, which can be larger than the array size limit.
    sql """
        INSERT INTO test_array_range_null_argument VALUES
            (1, 1, 4, 1, '2024-01-01 00:00:00.123456', '2024-01-01 00:00:03.123456'),
            (2, NULL, 5000000, 1, NULL, '2024-01-02 00:00:00'),
            (3, 2, NULL, 1, '2024-01-01 00:00:00', NULL),
            (4, 2, 8, NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:02'),
            (5, 0, 3, 2, NULL, NULL)
    """

    order_qt_int_range """
        SELECT id, array_range(s, e), array_range(s, e, st), sequence(s, e, st)
        FROM test_array_range_null_argument
    """
    order_qt_datetime_range """
        SELECT id, sequence(ds, de, INTERVAL 1 SECOND)
        FROM test_array_range_null_argument
    """
}
