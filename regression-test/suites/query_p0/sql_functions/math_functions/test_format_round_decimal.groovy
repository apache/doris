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

suite("test_format_round_decimal", "p0") {
    for (int precision : [9, 18, 20, 38]) {
        for (int places : [2, 9, 10, 18, 19, 20, 38, 39]) {
            order_qt_padding """
                SELECT format_round(CAST('1.44' AS DECIMAL(${precision}, 2)), ${places}),
                       format_round(CAST('-0.44' AS DECIMAL(${precision}, 2)), ${places}),
                       format_round(CAST('0.01' AS DECIMAL(${precision}, 2)), ${places})
            """
        }
    }

    order_qt_high_scale """
        SELECT format_round(CAST('1.12345678901234567' AS DECIMAL(18, 17)), 17),
               format_round(CAST('-1.12345678901234567' AS DECIMAL(18, 17)), 16),
               format_round(CAST('9.99999999999999999' AS DECIMAL(18, 17)), 10),
               format_round(CAST('-9.99999999999999999' AS DECIMAL(18, 17)), 10),
               format_round(CAST('0.10000000000000000001234567890123456789' AS DECIMAL(38, 38)), 38),
               format_round(CAST('-0.10000000000000000001234567890123456789' AS DECIMAL(38, 38)), 37),
               format_round(CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38, 38)), 37),
               format_round(CAST('-0.99999999999999999999999999999999999999' AS DECIMAL(38, 38)), 37)
    """

    order_qt_money_format """
        SELECT money_format(CAST('1.1249' AS DOUBLE)),
               money_format(CAST(CONCAT('1.124', REPEAT('9', 5)) AS DECIMAL(9, 8))),
               money_format(CAST(CONCAT('1.124', REPEAT('9', 14)) AS DECIMAL(18, 17))),
               money_format(CAST(CONCAT('1.124', REPEAT('9', 34)) AS DECIMAL(38, 37)))
    """

    sql "DROP TABLE IF EXISTS test_format_round_decimal"
    sql """
        CREATE TABLE test_format_round_decimal (
            id INT,
            d32 DECIMAL(9, 2),
            d64 DECIMAL(18, 2),
            d128 DECIMAL(38, 2),
            places INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_format_round_decimal VALUES
            (1, 1.44, 1.44, 1.44, 20),
            (2, -0.44, -0.44, -0.44, 39),
            (3, 0.01, 0.01, 0.01, 1024),
            (4, 9999.99, 9999.99, 9999.99, 1),
            (5, -9999.99, -9999.99, -9999.99, 0),
            (6, 0, 0, 0, 20),
            (7, NULL, NULL, NULL, 20),
            (8, 1.44, 1.44, 1.44, NULL)
    """
    order_qt_column_places """
        SELECT id, format_round(d32, places), format_round(d64, places),
               format_round(d128, places)
        FROM test_format_round_decimal
    """
    order_qt_constant_places """
        SELECT id, format_round(d32, 20), format_round(d64, 20), format_round(d128, 20)
        FROM test_format_round_decimal
    """
    order_qt_constant_value """
        SELECT id, format_round(CAST('1.44' AS DECIMAL(38, 2)), places)
        FROM test_format_round_decimal
    """
    order_qt_integer """
        SELECT format_round(CAST('-1234' AS BIGINT), 20),
               format_round(CAST('1234' AS LARGEINT), 1024)
    """

    test {
        sql "SELECT format_round(CAST('1.44' AS DECIMAL(38, 2)), -1)"
        exception "it should be in range [0, 1024]"
    }
    test {
        sql "SELECT format_round(CAST('1.44' AS DECIMAL(38, 2)), 1025)"
        exception "it should be in range [0, 1024]"
    }
}
