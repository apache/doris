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

suite("test_aggregate_group_array"){
    sql "set enable_vectorized_engine = true"

    def tableName = "group_uniq_array_test"
    def tableCTAS1 = "group_uniq_array_test_ctas1"
    def tableCTAS2 = "group_uniq_array_test_ctas2"

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
            c_char CHAR,
            c_varchar VARCHAR(10),
            c_string STRING,
            c_date DATE,
            c_datev2 DATEV2,
            c_date_time DATETIME,
            c_date_timev2 DATETIMEV2(6),
            c_string_not_null VARCHAR(10) NOT NULL
	    )
	    DISTRIBUTED BY HASH(c_int) BUCKETS 1
	    PROPERTIES (
	      "replication_num" = "1"
	    )
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (1, false, 10, 20, 30, 4444444444444, 55555555555, 0.1, 0.222, 3333.33, 'c', 'varchar1', 'string1',
            '2022-12-01', '2022-12-01', '2022-12-01 22:23:23', '2022-12-01 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (1, false, 11, 21, 33, 4444444444444, 55555555555, 0.1, 0.222, 3333.33, 'c', 'varchar1', 'string1',
            '2022-12-01', '2022-12-01', '2022-12-01 22:23:23', '2022-12-01 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (1, true, 11, 12, 13, 1444444444444, 1555555555, 1.1, 1.222, 13333.33, 'd', 'varchar2', 'string2',
            '2022-12-02', '2022-12-02', '2022-12-02 22:23:23', '2022-12-02 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (2, false, 21, 22, 23, 2444444444444, 255555555, 2.1, 2.222, 23333.33, 'f', 'varchar3', 'string3',
            '2022-12-03', '2022-12-03', '2022-12-03 22:23:23', '2022-12-03 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (2, true, 31, 32, 33, 3444444444444, 3555555555, 3.1, 3.222, 33333.33, 'l', 'varchar3', 'string3',
            '2022-12-03', '2022-12-03', '2022-12-03 22:23:23', '2022-12-03 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName} values
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (2, false, 10, 20, 30, 944444444444, 9555555555, 9.1, 9.222, 93333.33, 'p', 'varchar9', 'string9',
            '2022-12-09', '2022-12-09', '2022-12-09 22:23:23', '2022-12-09 22:23:24.999999', 'not null')
    """

    qt_select """
        SELECT
            group_uniq_array(c_bool),
            group_uniq_array(c_tinyint),
            group_uniq_array(c_smallint),
            group_uniq_array(c_int),
            group_uniq_array(c_bigint),
            group_uniq_array(c_largeint),
            group_uniq_array(c_float),
            group_uniq_array(c_double),
            group_uniq_array(c_decimal),
            group_uniq_array(c_char),
            group_uniq_array(c_varchar),
            group_uniq_array(c_string),
            group_uniq_array(c_date),
            group_uniq_array(c_datev2),
            group_uniq_array(c_date_time),
            group_uniq_array(c_date_timev2),
            group_uniq_array(c_string_not_null)
        FROM
            ${tableName}
    """

    qt_select """
        SELECT
            group_uniq_array(c_bool,1),
            group_uniq_array(c_tinyint,1),
            group_uniq_array(c_smallint,1),
            group_uniq_array(c_int,1),
            group_uniq_array(c_bigint,1),
            group_uniq_array(c_largeint,1),
            group_uniq_array(c_float,1),
            group_uniq_array(c_double,1),
            group_uniq_array(c_decimal,1),
            group_uniq_array(c_char,1),
            group_uniq_array(c_varchar,1),
            group_uniq_array(c_string,1),
            group_uniq_array(c_date,1),
            group_uniq_array(c_datev2,1),
            group_uniq_array(c_date_time,1),
            group_uniq_array(c_date_timev2,1),
            group_uniq_array(c_string_not_null,1)
        FROM
            ${tableName}
    """

    qt_select """
        SELECT
            group_uniq_array(c_bool),
            group_uniq_array(c_tinyint),
            group_uniq_array(c_smallint),
            group_uniq_array(c_int),
            group_uniq_array(c_bigint),
            group_uniq_array(c_largeint),
            group_uniq_array(c_float),
            group_uniq_array(c_double),
            group_uniq_array(c_decimal),
            group_uniq_array(c_char),
            group_uniq_array(c_varchar),
            group_uniq_array(c_string),
            group_uniq_array(c_date),
            group_uniq_array(c_datev2),
            group_uniq_array(c_date_time),
            group_uniq_array(c_date_timev2),
            group_uniq_array(c_string_not_null)
        FROM
            ${tableName}
        GROUP BY
            c_id
        ORDER BY
            c_id
    """

    qt_select """
        SELECT
            group_uniq_array(c_bool,1),
            group_uniq_array(c_tinyint,1),
            group_uniq_array(c_smallint,1),
            group_uniq_array(c_int,1),
            group_uniq_array(c_bigint,1),
            group_uniq_array(c_largeint,1),
            group_uniq_array(c_float,1),
            group_uniq_array(c_double,1),
            group_uniq_array(c_decimal,1),
            group_uniq_array(c_char,1),
            group_uniq_array(c_varchar,1),
            group_uniq_array(c_string,1),
            group_uniq_array(c_date,1),
            group_uniq_array(c_datev2,1),
            group_uniq_array(c_date_time,1),
            group_uniq_array(c_date_timev2,1),
            group_uniq_array(c_string_not_null,1)
        FROM
            ${tableName}
        GROUP BY
            c_id
        ORDER BY
            c_id
    """

    sql """
        CREATE TABLE ${tableCTAS1} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            group_uniq_array(c_bool),
            group_uniq_array(c_tinyint),
            group_uniq_array(c_smallint),
            group_uniq_array(c_int),
            group_uniq_array(c_bigint),
            group_uniq_array(c_largeint),
            group_uniq_array(c_float),
            group_uniq_array(c_double),
            group_uniq_array(c_decimal),
            group_uniq_array(c_char),
            group_uniq_array(c_varchar),
            group_uniq_array(c_string),
            group_uniq_array(c_date),
            group_uniq_array(c_datev2),
            group_uniq_array(c_date_time),
            group_uniq_array(c_date_timev2),
            group_uniq_array(c_string_not_null)
        FROM
            ${tableName}
    """


    sql """
        CREATE TABLE ${tableCTAS2} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            group_uniq_array(c_bool,1),
            group_uniq_array(c_tinyint,1),
            group_uniq_array(c_smallint,1),
            group_uniq_array(c_int,1),
            group_uniq_array(c_bigint,1),
            group_uniq_array(c_largeint,1),
            group_uniq_array(c_float,1),
            group_uniq_array(c_double,1),
            group_uniq_array(c_decimal,1),
            group_uniq_array(c_char,1),
            group_uniq_array(c_varchar,1),
            group_uniq_array(c_string,1),
            group_uniq_array(c_date,1),
            group_uniq_array(c_datev2,1),
            group_uniq_array(c_date_time,1),
            group_uniq_array(c_date_timev2,1),
            group_uniq_array(c_string_not_null,1)
        FROM
            ${tableName}
    """

    qt_select "SELECT * FROM ${tableCTAS1}"
    qt_select "SELECT * FROM ${tableCTAS2}"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"
}
