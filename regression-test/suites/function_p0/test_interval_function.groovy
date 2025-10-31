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

    qt_interval_basic_1 """ SELECT `INTERVAL`(23, 1, 15, 17, 30, 44, 200); """
    qt_interval_basic_2 """ SELECT `INTERVAL`(10, 1, 10, 100, 1000); """
    qt_interval_basic_3 """ SELECT `INTERVAL`(22, 23, 30, 44, 200); """
    qt_interval_basic_4 """ SELECT `INTERVAL`(33, 1, 10, 32, 32, 102, 200); """
    qt_interval_basic_5 """ SELECT `INTERVAL`(33, 1, 10, 32, 33, 102, 200); """

    qt_interval_boundary_min """ SELECT `INTERVAL`(0, 1, 10, 100); """
    qt_interval_boundary_first """ SELECT `INTERVAL`(1, 1, 10, 100); """
    qt_interval_boundary_last """ SELECT `INTERVAL`(100, 1, 10, 100); """
    qt_interval_boundary_max """ SELECT `INTERVAL`(200, 1, 10, 100); """
    qt_interval_boundary_between_1 """ SELECT `INTERVAL`(5, 1, 10, 100); """
    qt_interval_boundary_between_2 """ SELECT `INTERVAL`(50, 1, 10, 100); """

    qt_interval_negative_1 """ SELECT `INTERVAL`(-10, -100, -50, -10, 0, 50, 100); """
    qt_interval_negative_2 """ SELECT `INTERVAL`(-5, -100, -50, -10, 0, 50, 100); """
    qt_interval_negative_3 """ SELECT `INTERVAL`(5, -100, -50, -10, 0, 50, 100); """
    qt_interval_negative_4 """ SELECT `INTERVAL`(0, -100, -50, -10, 0, 50, 100); """

    qt_interval_duplicate_thresholds_1 """ SELECT `INTERVAL`(10, 1, 10, 10, 20, 20, 30); """
    qt_interval_duplicate_thresholds_2 """ SELECT `INTERVAL`(15, 1, 10, 10, 20, 20, 30); """
    qt_interval_duplicate_thresholds_3 """ SELECT `INTERVAL`(25, 1, 10, 10, 20, 20, 30); """

    qt_interval_single_threshold_1 """ SELECT `INTERVAL`(0, 10); """
    qt_interval_single_threshold_2 """ SELECT `INTERVAL`(10, 10); """
    qt_interval_single_threshold_3 """ SELECT `INTERVAL`(20, 10); """

    qt_interval_two_thresholds_1 """ SELECT `INTERVAL`(0, 10, 20); """
    qt_interval_two_thresholds_2 """ SELECT `INTERVAL`(10, 10, 20); """
    qt_interval_two_thresholds_3 """ SELECT `INTERVAL`(15, 10, 20); """
    qt_interval_two_thresholds_4 """ SELECT `INTERVAL`(20, 10, 20); """
    qt_interval_two_thresholds_5 """ SELECT `INTERVAL`(30, 10, 20); """

    qt_interval_tinyint """ SELECT `INTERVAL`(CAST(5 AS TINYINT), CAST(1 AS TINYINT), CAST(10 AS TINYINT), CAST(20 AS TINYINT)); """
    qt_interval_smallint """ SELECT `INTERVAL`(CAST(15 AS SMALLINT), CAST(1 AS SMALLINT), CAST(10 AS SMALLINT), CAST(20 AS SMALLINT)); """
    qt_interval_int """ SELECT `INTERVAL`(CAST(15 AS INT), CAST(1 AS INT), CAST(10 AS INT), CAST(20 AS INT)); """
    qt_interval_bigint """ SELECT `INTERVAL`(CAST(15 AS BIGINT), CAST(1 AS BIGINT), CAST(10 AS BIGINT), CAST(20 AS BIGINT)); """
    qt_interval_largeint """ SELECT `INTERVAL`(CAST(15 AS LARGEINT), CAST(1 AS LARGEINT), CAST(10 AS LARGEINT), CAST(20 AS LARGEINT)); """

    qt_interval_null_first_arg """ SELECT `INTERVAL`(NULL, 1, 10, 100); """
    qt_interval_null_threshold """ SELECT `INTERVAL`(50, NULL, 10, 100); """

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
        SELECT id, `INTERVAL`(val_int, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_tinyint """
        SELECT id, `INTERVAL`(val_tinyint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_smallint """
        SELECT id, `INTERVAL`(val_smallint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_bigint """
        SELECT id, `INTERVAL`(val_bigint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_from_table_largeint """
        SELECT id, `INTERVAL`(val_largeint, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_with_const_thresholds """
        SELECT id, `INTERVAL`(val_int, 10, 20, 30, 40, 50) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_with_const_value """
        SELECT id, `INTERVAL`(25, thresh1, thresh2, thresh3, thresh4, thresh5) as result
        FROM ${intervalTestTable}
        WHERE id <= 7
        ORDER BY id;
    """

    qt_interval_complex_1 """
        SELECT `INTERVAL`(100, 1, 10, 50, 100, 200, 500, 1000);
    """

    qt_interval_complex_2 """
        SELECT `INTERVAL`(0, 1, 10, 50, 100, 200, 500, 1000);
    """

    qt_interval_complex_3 """
        SELECT `INTERVAL`(1000, 1, 10, 50, 100, 200, 500, 1000);
    """

    qt_interval_complex_4 """
        SELECT `INTERVAL`(1001, 1, 10, 50, 100, 200, 500, 1000);
    """

    sql """ DROP TABLE IF EXISTS ${intervalTestTable}; """
}

