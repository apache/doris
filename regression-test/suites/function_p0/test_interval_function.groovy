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

suite("test_interval_function") {

    qt_interval_basic_1 """ SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200); """
    qt_interval_basic_2 """ SELECT INTERVAL(10, 1, 10, 100, 1000); """
    qt_interval_basic_3 """ SELECT INTERVAL(22, 23, 30, 44, 200); """
    qt_interval_basic_4 """ SELECT INTERVAL(33, 1, 10, 32, 32, 102, 200); """
    qt_interval_basic_5 """ SELECT INTERVAL(33, 1, 10, 32, 33, 102, 200); """

    qt_interval_boundary_min """ SELECT INTERVAL(0, 1, 10, 100); """
    qt_interval_boundary_first """ SELECT INTERVAL(1, 1, 10, 100); """
    qt_interval_boundary_last """ SELECT INTERVAL(100, 1, 10, 100); """
    qt_interval_boundary_max """ SELECT INTERVAL(200, 1, 10, 100); """
    qt_interval_boundary_between_1 """ SELECT INTERVAL(5, 1, 10, 100); """
    qt_interval_boundary_between_2 """ SELECT INTERVAL(50, 1, 10, 100); """

    qt_interval_negative_1 """ SELECT INTERVAL(-10, -100, -50, -10, 0, 50, 100); """
    qt_interval_negative_2 """ SELECT INTERVAL(-5, -100, -50, -10, 0, 50, 100); """
    qt_interval_negative_3 """ SELECT INTERVAL(5, -100, -50, -10, 0, 50, 100); """
    qt_interval_negative_4 """ SELECT INTERVAL(0, -100, -50, -10, 0, 50, 100); """

    qt_interval_duplicate_thresholds_1 """ SELECT INTERVAL(10, 1, 10, 10, 20, 20, 30); """
    qt_interval_duplicate_thresholds_2 """ SELECT INTERVAL(15, 1, 10, 10, 20, 20, 30); """
    qt_interval_duplicate_thresholds_3 """ SELECT INTERVAL(25, 1, 10, 10, 20, 20, 30); """

    qt_interval_single_threshold_1 """ SELECT INTERVAL(0, 10); """
    qt_interval_single_threshold_2 """ SELECT INTERVAL(10, 10); """
    qt_interval_single_threshold_3 """ SELECT INTERVAL(20, 10); """

    qt_interval_two_thresholds_1 """ SELECT INTERVAL(0, 10, 20); """
    qt_interval_two_thresholds_2 """ SELECT INTERVAL(10, 10, 20); """
    qt_interval_two_thresholds_3 """ SELECT INTERVAL(15, 10, 20); """
    qt_interval_two_thresholds_4 """ SELECT INTERVAL(20, 10, 20); """
    qt_interval_two_thresholds_5 """ SELECT INTERVAL(30, 10, 20); """

    qt_interval_null_first_arg """ SELECT INTERVAL(NULL, 1, 10, 100); """
    qt_interval_null_threshold """ SELECT INTERVAL(50, NULL, 10, 100); """

    // Value not NULL, thresholds partially or fully NULL
    qt_interval_thresh_partial_null_1 """ SELECT INTERVAL(50, NULL, 20, 100); """
    qt_interval_thresh_partial_null_2 """ SELECT INTERVAL(50, 10, NULL, 100); """
    qt_interval_thresh_partial_null_3 """ SELECT INTERVAL(50, 10, 20, NULL); """
    qt_interval_thresh_partial_null_4    """ SELECT INTERVAL(50, NULL, NULL, NULL); """

    def intervalTestTable = "interval_function_test_table"

    sql """ DROP TABLE IF EXISTS ${intervalTestTable}; """

    sql """
        CREATE TABLE IF NOT EXISTS ${intervalTestTable} (
            id INT,
            val_tinyint TINYINT,
            val_smallint SMALLINT,
            val_int INT,
            val_bigint BIGINT,
            val_largeint LARGEINT,
            thresh1 INT,
            thresh2 INT,
            thresh3 INT,
            thresh4 INT,
            thresh5 INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${intervalTestTable} VALUES
        (1, 5, 15, 25, 35, 45, 10, 20, 30, 40, 50),
        (2, 15, 25, 35, 45, 55, 10, 20, 30, 40, 50),
        (3, 25, 35, 45, 55, 65, 10, 20, 30, 40, 50),
        (4, 0, 5, 5, 5, 5, 10, 20, 30, 40, 50),
        (5, 60, 60, 60, 60, 60, 10, 20, 30, 40, 50),
        (6, 10, 20, 30, 40, 50, 10, 20, 30, 40, 50),
        (7, -10, -5, 0, 5, 10, -20, -10, 0, 10, 20),
        (8, NULL, NULL, NULL, NULL, NULL, 10, 20, 30, 40, 50)
    """

    qt_interval_from_table_int """
        SELECT id, INTERVAL(val_int, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_tinyint """
        SELECT id, INTERVAL(val_tinyint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_smallint """
        SELECT id, INTERVAL(val_smallint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_bigint """
        SELECT id, INTERVAL(val_bigint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_largeint """
        SELECT id, INTERVAL(val_largeint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_with_const_thresholds """
        SELECT id, INTERVAL(val_int, 10, 20, 30, 40, 50) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_with_const_value """
        SELECT id, INTERVAL(25, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    // Value from column, thresholds with mixed column and NULL
    qt_interval_thresh_partial_null_tbl_1 """
        SELECT id, INTERVAL(val_int, NULL, thresh2, thresh3) AS result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_thresh_partial_null_tbl_2 """
        SELECT id, INTERVAL(val_int, thresh1, NULL, thresh3) AS result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_thresh_partial_null_tbl_3 """
        SELECT id, INTERVAL(val_int, thresh1, thresh2, NULL) AS result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_thresh_partial_null_tbl_4 """
        SELECT id, INTERVAL(val_int, NULL, NULL, NULL) AS result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    // Mixed thresholds: some constants, some columns
    qt_interval_mixed_thresholds_1 """
        SELECT id, INTERVAL(val_int, thresh1, 20, thresh3, 40, 50) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_mixed_thresholds_2 """
        SELECT id, INTERVAL(val_int, 10, thresh2, thresh3, 40, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_mixed_thresholds_3 """
        SELECT id, INTERVAL(val_int, 10, 20, thresh3, thresh4, 50) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_const_value_mixed_thresholds """
        SELECT id, INTERVAL(25, thresh1, 20, thresh3, 40, 50) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_complex_1 """
        SELECT INTERVAL(100, 1, 10, 50, 100, 200, 500, 1000);
    """

    qt_interval_complex_2 """
        SELECT INTERVAL(0, 1, 10, 50, 100, 200, 500, 1000);
    """

    qt_interval_complex_3 """
        SELECT INTERVAL(1000, 1, 10, 50, 100, 200, 500, 1000);
    """

    qt_interval_complex_4 """
        SELECT INTERVAL(1001, 1, 10, 50, 100, 200, 500, 1000);
    """

    sql """ DROP TABLE IF EXISTS ${intervalTestTable}; """

    testFoldConst("SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200);")
    testFoldConst("SELECT INTERVAL(10, 1, 10, 100, 1000);")
    testFoldConst("SELECT INTERVAL(22, 23, 30, 44, 200);")
    testFoldConst("SELECT INTERVAL(33, 1, 10, 32, 32, 102, 200);")
    testFoldConst("SELECT INTERVAL(33, 1, 10, 32, 33, 102, 200);")

    testFoldConst("SELECT INTERVAL(0, 1, 10, 100);")
    testFoldConst("SELECT INTERVAL(1, 1, 10, 100);")
    testFoldConst("SELECT INTERVAL(100, 1, 10, 100);")
    testFoldConst("SELECT INTERVAL(200, 1, 10, 100);")
    testFoldConst("SELECT INTERVAL(5, 1, 10, 100);")
    testFoldConst("SELECT INTERVAL(50, 1, 10, 100);")

    testFoldConst("SELECT INTERVAL(-10, -100, -50, -10, 0, 50, 100);")
    testFoldConst("SELECT INTERVAL(-5, -100, -50, -10, 0, 50, 100);")
    testFoldConst("SELECT INTERVAL(5, -100, -50, -10, 0, 50, 100);")
    testFoldConst("SELECT INTERVAL(0, -100, -50, -10, 0, 50, 100);")

    testFoldConst("SELECT INTERVAL(10, 1, 10, 10, 20, 20, 30);")
    testFoldConst("SELECT INTERVAL(15, 1, 10, 10, 20, 20, 30);")
    testFoldConst("SELECT INTERVAL(25, 1, 10, 10, 20, 20, 30);")

    testFoldConst("SELECT INTERVAL(0, 10);")
    testFoldConst("SELECT INTERVAL(10, 10);")
    testFoldConst("SELECT INTERVAL(20, 10);")

    testFoldConst("SELECT INTERVAL(0, 10, 20);")
    testFoldConst("SELECT INTERVAL(10, 10, 20);")
    testFoldConst("SELECT INTERVAL(15, 10, 20);")
    testFoldConst("SELECT INTERVAL(20, 10, 20);")
    testFoldConst("SELECT INTERVAL(30, 10, 20);")

    testFoldConst("SELECT INTERVAL(NULL, 1, 10, 100);")
    testFoldConst("SELECT INTERVAL(50, NULL, 10, 100);")
    testFoldConst("SELECT INTERVAL(50, NULL, 20, 100);")
    testFoldConst("SELECT INTERVAL(50, 10, NULL, 100);")
    testFoldConst("SELECT INTERVAL(50, 10, 20, NULL);")
    testFoldConst("SELECT INTERVAL(50, NULL, NULL, NULL);")

    testFoldConst("SELECT INTERVAL(100, 1, 10, 50, 100, 200, 500, 1000);")
    testFoldConst("SELECT INTERVAL(0, 1, 10, 50, 100, 200, 500, 1000);")
    testFoldConst("SELECT INTERVAL(1000, 1, 10, 50, 100, 200, 500, 1000);")
    testFoldConst("SELECT INTERVAL(1001, 1, 10, 50, 100, 200, 500, 1000);")
}

