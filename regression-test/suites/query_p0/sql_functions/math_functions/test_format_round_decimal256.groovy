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

suite("test_format_round_decimal256", "p0") {
    sql "SET enable_decimal256 = true"

    sql "DROP TABLE IF EXISTS test_format_round_decimal256"
    sql """
        CREATE TABLE test_format_round_decimal256 (
            id INT,
            f1 DECIMAL(76, 2),
            places INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_format_round_decimal256 VALUES
            (1, 1.44, 20),
            (2, -0.44, 76),
            (3, 0.01, 1024),
            (4, CONCAT(REPEAT('9', 74), '.99'), 1),
            (5, CONCAT('-', REPEAT('9', 74), '.99'), 0),
            (6, 0, 77),
            (7, NULL, 20),
            (8, 1.44, NULL)
    """
    order_qt_repro "SELECT format_round(f1, 20) FROM test_format_round_decimal256 WHERE id = 1"
    order_qt_column_places """
        SELECT id, format_round(f1, places) FROM test_format_round_decimal256
    """
    order_qt_constant_places """
        SELECT id, format_round(f1, 20) FROM test_format_round_decimal256
    """
    order_qt_constant_value """
        SELECT id, format_round(CAST('1.44' AS DECIMAL(76, 2)), places)
        FROM test_format_round_decimal256
    """

    sql "DROP TABLE IF EXISTS test_format_round_decimal256_high_scale"
    sql """
        CREATE TABLE test_format_round_decimal256_high_scale (
            id INT,
            f1 DECIMAL(76, 76),
            places INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_format_round_decimal256_high_scale VALUES
            (1, CONCAT('0.', REPEAT('9', 76)), 0),
            (2, CONCAT('0.', REPEAT('9', 76)), 20),
            (3, CONCAT('0.', REPEAT('9', 76)), 75),
            (4, CONCAT('-0.', REPEAT('9', 76)), 75),
            (5, CONCAT('0.', REPEAT('9', 76)), 76),
            (6, CONCAT('0.', REPEAT('9', 76)), 1024),
            (7, CONCAT('0.1', REPEAT('0', 56), '1234567890123456789'), 76),
            (8, CONCAT('-0.1', REPEAT('0', 56), '1234567890123456789'), 75),
            (9, CONCAT('0.', REPEAT('0', 75), '1'), 76),
            (10, CONCAT('-0.', REPEAT('0', 75), '1'), 75)
    """
    order_qt_high_scale_columns """
        SELECT id, format_round(f1, places) FROM test_format_round_decimal256_high_scale
    """
    for (int places : [0, 1, 20, 38, 39, 40, 74, 75, 76, 77, 1024]) {
        order_qt_high_scale_constants """
            SELECT format_round(CAST(CONCAT('0.', REPEAT('9', 76)) AS DECIMAL(76, 76)), ${places}),
                   format_round(CAST(CONCAT('-0.', REPEAT('9', 76)) AS DECIMAL(76, 76)), ${places})
        """
    }
    order_qt_large_integer """
        SELECT format_round(CAST(REPEAT('9', 76) AS DECIMAL(76, 0)), 0),
               format_round(CAST(CONCAT('-', REPEAT('9', 76)) AS DECIMAL(76, 0)), 20)
    """
    order_qt_smallest_decimal256 """
        SELECT format_round(CAST('1.44' AS DECIMAL(39, 2)), 20),
               format_round(CAST(CONCAT('0.', REPEAT('9', 39)) AS DECIMAL(39, 39)), 38)
    """

    test {
        sql "SELECT format_round(CAST('1.44' AS DECIMAL(76, 2)), -1)"
        exception "it should be in range [0, 1024]"
    }
    test {
        sql "SELECT format_round(CAST('1.44' AS DECIMAL(76, 2)), 1025)"
        exception "it should be in range [0, 1024]"
    }
}
