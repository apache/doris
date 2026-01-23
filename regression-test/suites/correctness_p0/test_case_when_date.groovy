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

suite("test_case_when_date") {
    sql """ DROP TABLE IF EXISTS test_case_when_date_tbl """
    sql """
        CREATE TABLE test_case_when_date_tbl
        (
            id INT NOT NULL,
            col_date DATE NULL,
            col_date_not_null DATE NOT NULL,
            col_datev2 DATEV2 NULL,
            col_datev2_not_null DATEV2 NOT NULL,
            col_datetime DATETIME NULL,
            col_datetime_not_null DATETIME NOT NULL,
            col_datetimev2 DATETIMEV2 NULL,
            col_datetimev2_not_null DATETIMEV2 NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    // Insert test data with various date values
    sql """
        INSERT INTO test_case_when_date_tbl VALUES
        (1, '2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01 10:00:00'),
        (2, '2023-02-15', '2023-02-15', '2023-02-15', '2023-02-15', '2023-02-15 12:30:00', '2023-02-15 12:30:00', '2023-02-15 12:30:00', '2023-02-15 12:30:00'),
        (3, '2023-03-20', '2023-03-20', '2023-03-20', '2023-03-20', '2023-03-20 15:45:00', '2023-03-20 15:45:00', '2023-03-20 15:45:00', '2023-03-20 15:45:00'),
        (4, NULL, '2023-04-10', NULL, '2023-04-10', NULL, '2023-04-10 08:00:00', NULL, '2023-04-10 08:00:00'),
        (5, '2023-05-25', '2023-05-25', '2023-05-25', '2023-05-25', '2023-05-25 20:00:00', '2023-05-25 20:00:00', '2023-05-25 20:00:00', '2023-05-25 20:00:00');
    """

    // Test 1: CASE WHEN with multiple branches on DATEV2, applying TO_DATE function
    // This was the original failing case - multi-branch CASE WHEN with date functions
    qt_case_when_to_date """
        SELECT id,
            TO_DATE(
                CASE
                    WHEN col_datev2 IN ('2023-01-01', '2023-02-15') THEN col_datev2_not_null
                    WHEN col_datev2 = '2023-03-20' THEN col_datev2_not_null
                    ELSE DATE_ADD(col_datev2_not_null, INTERVAL 1 DAY)
                END
            ) AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """

    // Test 2: CASE WHEN with 3 branches on DATE type
    qt_case_when_date_3branches """
        SELECT id,
            CASE
                WHEN id = 1 THEN col_date_not_null
                WHEN id = 2 THEN DATE_ADD(col_date_not_null, INTERVAL 10 DAY)
                ELSE col_date_not_null
            END AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """

    // Test 3: CASE WHEN with MONTH function on result
    qt_case_when_month """
        SELECT id,
            MONTH(
                CASE
                    WHEN id <= 2 THEN col_datev2_not_null
                    WHEN id = 3 THEN col_datev2_not_null
                    ELSE col_datev2_not_null
                END
            ) AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """

    // Test 4: CASE WHEN with DAYOFMONTH function
    qt_case_when_dayofmonth """
        SELECT id,
            DAYOFMONTH(
                CASE
                    WHEN id = 1 THEN col_datev2_not_null
                    WHEN id IN (2, 3) THEN col_datev2_not_null
                    ELSE col_datev2_not_null
                END
            ) AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """

    // Test 5: CASE WHEN on DATETIME type with multiple branches
    qt_case_when_datetime """
        SELECT id,
            CASE
                WHEN id = 1 THEN col_datetime_not_null
                WHEN id = 2 THEN col_datetime_not_null
                WHEN id = 3 THEN col_datetime_not_null
                ELSE col_datetime_not_null
            END AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """

    // Test 6: CASE WHEN on DATETIMEV2 with date functions
    qt_case_when_datetimev2_to_date """
        SELECT id,
            TO_DATE(
                CASE
                    WHEN id <= 2 THEN col_datetimev2_not_null
                    WHEN id = 3 THEN col_datetimev2_not_null
                    ELSE col_datetimev2_not_null
                END
            ) AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """

    // Test 7: Nested CASE WHEN with date types
    qt_case_when_nested """
        SELECT id,
            CASE
                WHEN id = 1 THEN
                    CASE
                        WHEN col_datev2 IS NOT NULL THEN col_datev2_not_null
                        ELSE col_datev2_not_null
                    END
                WHEN id = 2 THEN col_datev2_not_null
                ELSE col_datev2_not_null
            END AS result
        FROM test_case_when_date_tbl
        ORDER BY id;
    """
}
