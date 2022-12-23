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

suite("test_aggregate_histogram") {
    sql "set enable_vectorized_engine = true"

    def tableName = "histogram_test"
    def tableCTAS1 = "histogram_test_ctas1"
    def tableCTAS2 = "histogram_test_ctas2"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            c_int INT,
            c_float FLOAT,
            c_double DOUBLE,
            c_decimal DECIMAL,
            c_string STRING,
            c_date DATE,
            c_date_time DATETIME,
            c_string_not_null VARCHAR(10) NOT NULL
	    )
        DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item1_2'),
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item1_2'),
            (2, '0.1', '0.11111111', '11111111.02', 'item1_1', '2022-12-01', '2022-12-01 22:23:24', 'item1_2'),
            (2, '0.1', '0.11111111', '11111111.02', 'item1_1', '2022-12-01', '2022-12-01 22:23:24', 'item1_2')
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item2_2'),
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item2_2'),
            (2, '0.2', '0.22222222', '22222222.02', 'item1_1', '2022-12-02', '2022-12-02 22:23:24', 'item2_2'),
            (2, '0.2', '0.22222222', '22222222.02', 'item1_1', '2022-12-02', '2022-12-02 22:23:24', 'item2_2')
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item3_2'),
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item3_2'),
            (2, '0.3', '0.3333333', '3333333.03', 'item1_1', '2022-12-03', '2022-12-03 22:23:24', 'item3_2'),
            (2, '0.3', '0.3333333', '3333333.03', 'item1_1', '2022-12-03', '2022-12-03 22:23:24', 'item3_2')
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item4_2'),
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item4_2'),
            (2, '0.4', '0.4444444', '4444444.04', 'item1_1', '2022-12-04', '2022-12-04 22:23:24', 'item4_2'),
            (2, '0.4', '0.4444444', '4444444.04', 'item1_1', '2022-12-04', '2022-12-04 22:23:24', 'item4_2')
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item5_2'),
            (1, NULL, NULL, NULL, NULL, NULL, NULL, 'item5_2'),
            (2, '0.5', '0.55555555', '55555555.0', 'item1_1', '2022-12-05', '2022-12-05 22:23:24', 'item5_2'),
            (2, '0.5', '0.55555555', '55555555.0', 'item1_1', '2022-12-05', '2022-12-05 22:23:24', 'item5_2')
    """

    // Test without input parameters and no GROUP BY
    qt_select """
        SELECT 
            histogram(c_int),
            histogram(c_float),
            histogram(c_double),
            histogram(c_decimal),
            histogram(c_string),
            histogram(c_date),
            histogram(c_date_time),
            histogram(c_string_not_null)
        FROM 
            ${tableName}
    """

    // Test without input parameters and with GROUP BY
    qt_select """
        SELECT 
            c_int,
            histogram(c_float),
            histogram(c_double),
            histogram(c_decimal),
            histogram(c_string),
            histogram(c_date),
            histogram(c_date_time),
            histogram(c_string_not_null)
        FROM 
            ${tableName}
        GROUP BY
            c_int
        ORDER BY
            c_int
    """

    // Test with input parameters and no GROUP BY
    qt_select """
        SELECT
            histogram(c_int, 0.5, 2),
            histogram(c_float, 0.5, 2),
            histogram(c_double, 0.5, 2),
            histogram(c_decimal, 0.5, 2),
            histogram(c_string, 0.5, 2),
            histogram(c_date, 0.5, 2),
            histogram(c_date_time, 0.5, 2),
            histogram(c_string_not_null, 0.5, 2)
        FROM
            ${tableName}
    """

    // Test with input parameters and with GROUP BY
    qt_select """
        SELECT
            c_int,
            histogram(c_float, 0.5, 2),
            histogram(c_double, 0.5, 2),
            histogram(c_decimal, 0.5, 2),
            histogram(c_string, 0.5, 2),
            histogram(c_date, 0.5, 2),
            histogram(c_date_time, 0.5, 2),
            histogram(c_string_not_null, 0.5, 2)
        FROM
            ${tableName}
        GROUP BY
            c_int
        ORDER BY
            c_int
    """

    sql """
        CREATE TABLE ${tableCTAS1} PROPERTIES("replication_num" = "1") AS
        SELECT
            1,
            histogram(c_float),
            histogram(c_double),
            histogram(c_decimal),
            histogram(c_string),
            histogram(c_date),
            histogram(c_date_time),
            histogram(c_string_not_null)
        FROM
            ${tableName}
    """

    sql """
        CREATE TABLE ${tableCTAS2} PROPERTIES("replication_num" = "1") AS
        SELECT
            1,
            histogram(c_float, 0.5, 2),
            histogram(c_double, 0.5, 2),
            histogram(c_decimal, 0.5, 2),
            histogram(c_string, 0.5, 2),
            histogram(c_date, 0.5, 2),
            histogram(c_date_time, 0.5, 2),
            histogram(c_string_not_null, 0.5, 2)
        FROM
            ${tableName}
    """

    qt_select "SELECT * from ${tableCTAS1}"
    qt_select "SELECT * from ${tableCTAS2}"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"
}
