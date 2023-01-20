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
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "histogram_test"
    def tableCTAS1 = "histogram_test_ctas1"
    def tableCTAS2 = "histogram_test_ctas2"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            c_id INT,
            c_bool BOOLEAN,
            c_tinyint TINYINT,
            c_smallint SMALLINT,
            c_int INT,
            c_bigint BIGINT,
            c_largeint LARGEINT,
            c_float FLOAT,
            c_double DOUBLE,
            c_decimal DECIMAL(9, 2),
            c_decimalv3 DECIMALV3(9, 2),
            c_char CHAR,
            c_varchar VARCHAR(10),
            c_string STRING,
            c_date DATE,
            c_datev2 DATEV2,
            c_date_time DATETIME,
            c_date_timev2 DATETIMEV2(6),
            c_string_not_null VARCHAR(10) NOT NULL
	    )
        DISTRIBUTED BY HASH(c_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${tableName} values 
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, 'not null'),
            (1, false, 10, 20, 30, 4444444444444, 55555555555, 0.1, 0.222, 3333.33, 4444.44, 'c', 'varchar1', 'string1', 
            '2022-12-01', '2022-12-01', '2022-12-01 22:23:23', '2022-12-01 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values 
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, 'not null'),
            (1, false, 11, 21, 33, 4444444444444, 55555555555, 0.1, 0.222, 3333.33, 4444.44, 'c', 'varchar1', 'string1', 
            '2022-12-01', '2022-12-01', '2022-12-01 22:23:23', '2022-12-01 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values 
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, 'not null'),
            (1, true, 11, 12, 13, 1444444444444, 1555555555, 1.1, 1.222, 13333.33, 14444.44, 'd', 'varchar2', 'string2', 
            '2022-12-02', '2022-12-02', '2022-12-02 22:23:23', '2022-12-02 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values 
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, 'not null'),
            (2, false, 21, 22, 23, 2444444444444, 255555555, 2.1, 2.222, 23333.33, 24444.44, 'f', 'varchar3', 'string3', 
            '2022-12-03', '2022-12-03', '2022-12-03 22:23:23', '2022-12-03 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values 
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, 'not null'),
            (2, true, 31, 32, 33, 3444444444444, 3555555555, 3.1, 3.222, 33333.33, 34444.44, 'l', 'varchar3', 'string3', 
            '2022-12-03', '2022-12-03', '2022-12-03 22:23:23', '2022-12-03 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values 
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            NULL, NULL, NULL, NULL, 'not null'),
            (2, false, 10, 20, 30, 944444444444, 9555555555, 9.1, 9.222, 93333.33, 94444.44, 'p', 'varchar9', 'string9', 
            '2022-12-09', '2022-12-09', '2022-12-09 22:23:23', '2022-12-09 22:23:24.999999', 'not null')
    """

    // Test without GROUP BY
    // Nereids does't support decimalV3 function
    // qt_select """
    //     SELECT
    //         `histogram`(c_bool, 1.0, 1), 
    //         `histogram`(c_tinyint, 1.0, 1), 
    //         `histogram`(c_smallint, 1.0, 1), 
    //         `histogram`(c_bigint, 1.0, 1), 
    //         `histogram`(c_largeint, 1.0, 1), 
    //         `histogram`(c_float, 1.0, 1), 
    //         `histogram`(c_double, 1.0, 1), 
    //         `histogram`(c_decimal, 1.0, 1), 
    //         `histogram`(c_decimalv3, 1.0, 1), 
    //         `histogram`(c_char, 1.0, 1), 
    //         `histogram`(c_varchar, 1.0, 1), 
    //         `histogram`(c_string, 1.0, 1), 
    //         `histogram`(c_date, 1.0, 1), 
    //         `histogram`(c_datev2, 1.0, 1), 
    //         `histogram`(c_date_time, 1.0, 1), 
    //         `histogram`(c_date_timev2, 1.0, 1), 
    //         `histogram`(c_string_not_null, 1.0, 1)
    //     FROM
    //         ${tableName}
    // """

    // Test with GROUP BY
    // Nereids does't support decimalV3 function
    // qt_select """
    //     SELECT
    //         c_id, 
    //         hist(c_bool, 1.0, 1), 
    //         hist(c_tinyint, 1.0, 1), 
    //         hist(c_smallint, 1.0, 1), 
    //         hist(c_bigint, 1.0, 1), 
    //         hist(c_largeint, 1.0, 1), 
    //         hist(c_float, 1.0, 1), 
    //         hist(c_double, 1.0, 1), 
    //         hist(c_decimal, 1.0, 1), 
    //         hist(c_decimalv3, 1.0, 1), 
    //         hist(c_char, 1.0, 1), 
    //         hist(c_varchar, 1.0, 1), 
    //         hist(c_string, 1.0, 1), 
    //         hist(c_date, 1.0, 1), 
    //         hist(c_datev2, 1.0, 1), 
    //         hist(c_date_time, 1.0, 1), 
    //         hist(c_date_timev2, 1.0, 1), 
    //         hist(c_string_not_null, 1.0, 1)
    //     FROM
    //         ${tableName}
    //     GROUP BY
    //         c_id
    //     ORDER BY
    //         c_id
    // """

    // Nereids does't support decimalV3 function
    // sql """
    //     CREATE TABLE ${tableCTAS1} PROPERTIES("replication_num" = "1") AS
    //     SELECT
    //         1, 
    //         hist(c_bool, 1.0, 2), 
    //         hist(c_tinyint, 1.0, 2), 
    //         hist(c_smallint, 1.0, 2), 
    //         hist(c_bigint, 1.0, 2), 
    //         hist(c_largeint, 1.0, 2), 
    //         hist(c_float, 1.0, 2), 
    //         hist(c_double, 1.0, 2), 
    //         hist(c_decimal, 1.0, 2), 
    //         hist(c_decimalv3, 1.0, 2), 
    //         hist(c_char, 1.0, 2), 
    //         hist(c_varchar, 1.0, 2), 
    //         hist(c_string, 1.0, 2), 
    //         hist(c_date, 1.0, 2), 
    //         hist(c_datev2, 1.0, 2), 
    //         hist(c_date_time, 1.0, 2), 
    //         hist(c_date_timev2, 1.0, 2), 
    //         hist(c_string_not_null, 1.0, 2)
    //     FROM
    //         ${tableName}
    // """

    // Nereids does't support decimalV3 function
    // sql """
    //     CREATE TABLE ${tableCTAS2} PROPERTIES("replication_num" = "1") AS
    //     SELECT
    //         1, 
    //         hist(c_bool, 1.0, 1), 
    //         hist(c_tinyint, 1.0, 1), 
    //         hist(c_smallint, 1.0, 1), 
    //         hist(c_bigint, 1.0, 1), 
    //         hist(c_largeint, 1.0, 1), 
    //         hist(c_float, 1.0, 1), 
    //         hist(c_double, 1.0, 1), 
    //         hist(c_decimal, 1.0, 1), 
    //         hist(c_decimalv3, 1.0, 1), 
    //         hist(c_char, 1.0, 1), 
    //         hist(c_varchar, 1.0, 1), 
    //         hist(c_string, 1.0, 1), 
    //         hist(c_date, 1.0, 1), 
    //         hist(c_datev2, 1.0, 1), 
    //         hist(c_date_time, 1.0, 1), 
    //         hist(c_date_timev2, 1.0, 1), 
    //         hist(c_string_not_null, 1.0, 1)
    //     FROM
    //         ${tableName}
    // """

    // Nereids does't support decimalV3 function
    // qt_select "SELECT * from ${tableCTAS1}"
    // Nereids does't support decimalV3 function
    // qt_select "SELECT * from ${tableCTAS2}"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"
}
