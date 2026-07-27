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

suite("test_datetimev2_nano_functions") {
    sql "DROP TABLE IF EXISTS test_datetimev2_nano_functions"
    sql """
        CREATE TABLE test_datetimev2_nano_functions (
            id INT,
            dt7 DATETIMEV2(7),
            dt8 DATETIMEV2(8),
            dt9 DATETIMEV2(9)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO test_datetimev2_nano_functions VALUES
        (1, '1969-12-31 23:59:59.9999999',
            '1969-12-31 23:59:59.99999999',
            '1969-12-31 23:59:59.999999999'),
        (2, '1970-01-01 00:00:00.0000001',
            '1970-01-01 00:00:00.00000001',
            '1970-01-01 00:00:00.000000001'),
        (3, '2024-02-29 12:34:56.1234567',
            '2024-02-29 12:34:56.12345678',
            '2024-02-29 12:34:56.123456789'),
        (4, '1677-09-21 00:12:43.1452242',
            '1677-09-21 00:12:43.14522420',
            '1677-09-21 00:12:43.145224192'),
        (5, '2262-04-11 23:47:16.8547758',
            '2262-04-11 23:47:16.85477580',
            '2262-04-11 23:47:16.854775807')
    """

    order_qt_floor_ceil """
        SELECT id,
               year_floor(dt9), quarter_floor(dt9), month_floor(dt9), week_floor(dt9),
               day_floor(dt9), hour_floor(dt9), minute_floor(dt9), second_floor(dt9),
               year_ceil(dt9), quarter_ceil(dt9), month_ceil(dt9), week_ceil(dt9),
               day_ceil(dt9), hour_ceil(dt9), minute_ceil(dt9), second_ceil(dt9)
        FROM test_datetimev2_nano_functions
        WHERE id BETWEEN 1 AND 3
        ORDER BY id
    """

    order_qt_scale_preservation """
        SELECT id,
               second_floor(dt7), second_ceil(dt7),
               second_floor(dt8), second_ceil(dt8),
               second_floor(dt9), second_ceil(dt9),
               date_trunc(dt7, 'second'),
               date_trunc(dt8, 'second'),
               date_trunc(dt9, 'second'),
               last_day(dt9),
               to_monday(dt9)
        FROM test_datetimev2_nano_functions
        WHERE id BETWEEN 1 AND 3
        ORDER BY id
    """

    order_qt_period_and_origin """
        SELECT id,
               second_floor(dt9, 2),
               second_ceil(dt9, 2),
               second_floor(dt9, CAST('1970-01-01 00:00:00.500000000' AS DATETIMEV2(9))),
               second_ceil(dt9, 2,
                   CAST('1970-01-01 00:00:00.500000000' AS DATETIMEV2(9)))
        FROM test_datetimev2_nano_functions
        WHERE id BETWEEN 1 AND 3
        ORDER BY id
    """

    qt_sequence_epoch """
        SELECT sequence(
            CAST('1969-12-31 23:59:58.000000001' AS DATETIMEV2(9)),
            CAST('1970-01-01 00:00:02.000000001' AS DATETIMEV2(9)),
            INTERVAL 1 SECOND)
    """

    qt_sequence_month """
        SELECT sequence(
            CAST('2024-01-31 12:34:56.123456789' AS DATETIMEV2(9)),
            CAST('2024-05-01 12:34:56.123456789' AS DATETIMEV2(9)),
            INTERVAL 1 MONTH)
    """

    qt_upper_boundary """
        SELECT second_floor(dt9)
        FROM test_datetimev2_nano_functions
        WHERE id = 5
    """

    sql "SET time_zone = '+08:00'"
    order_qt_timestamptz_cast """
        SELECT id,
               CAST(CAST(dt9 AS TIMESTAMPTZ(6)) AS STRING),
               CAST(CAST(dt9 AS TIMESTAMPTZ(6)) AS DATETIMEV2(9))
        FROM test_datetimev2_nano_functions
        WHERE id BETWEEN 1 AND 3
        ORDER BY id
    """

    test {
        sql """
            SELECT second_floor(dt9)
            FROM test_datetimev2_nano_functions
            WHERE id = 4
        """
        exception "out of range"
    }

    test {
        sql """
            SELECT date_trunc(dt9, 'second')
            FROM test_datetimev2_nano_functions
            WHERE id = 4
        """
        exception "out of range"
    }

    test {
        sql """
            SELECT second_ceil(dt9)
            FROM test_datetimev2_nano_functions
            WHERE id = 5
        """
        exception "out of range"
    }
}
