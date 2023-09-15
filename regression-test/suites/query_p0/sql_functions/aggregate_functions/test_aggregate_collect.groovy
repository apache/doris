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

suite("test_aggregate_collect") {
    def tableName = "collect_test"
    def tableCTAS1 = "collect_set_test_ctas1"
    def tableCTAS2 = "collect_set_test_ctas2"
    def tableCTAS3 = "collect_list_test_ctas3"
    def tableCTAS4 = "collect_list_test_ctas4"


    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"
    sql "DROP TABLE IF EXISTS ${tableCTAS3}"
    sql "DROP TABLE IF EXISTS ${tableCTAS4}"

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

    sql """
        SELECT * FROM ${tableName}
    """

    sql """
        SELECT
            collect_set(c_bool),
            collect_set(c_tinyint),
            collect_set(c_smallint),
            collect_set(c_int),
            collect_set(c_bigint),
            collect_set(c_largeint),
            collect_set(c_float),
            collect_set(c_double),
            collect_set(c_decimal),
            collect_set(c_char),
            collect_set(c_varchar),
            collect_set(c_string),
            collect_set(c_date),
            collect_set(c_datev2),
            collect_set(c_date_time),
            collect_set(c_date_timev2),
            collect_set(c_string_not_null)
        FROM
            ${tableName}
    """

    sql """
        SELECT
            collect_list(c_bool),
            collect_list(c_tinyint),
            collect_list(c_smallint),
            collect_list(c_int),
            collect_list(c_bigint),
            collect_list(c_largeint),
            collect_list(c_float),
            collect_list(c_double),
            collect_list(c_decimal),
            collect_list(c_char),
            collect_list(c_varchar),
            collect_list(c_string),
            collect_list(c_date),
            collect_list(c_datev2),
            collect_list(c_date_time),
            collect_list(c_date_timev2),
            collect_list(c_string_not_null)
        FROM
            ${tableName}
    """

    order_qt_select """
        SELECT
            size(collect_set(c_bool,1)),
            size(collect_set(c_tinyint,1)),
            size(collect_set(c_smallint,1)),
            size(collect_set(c_int,1)),
            size(collect_set(c_bigint,1)),
            size(collect_set(c_largeint,3)),
            size(collect_set(c_float,1)),
            size(collect_set(c_double,2)),
            size(collect_set(c_decimal,1)),
            size(collect_set(c_char,1)),
            size(collect_set(c_varchar,1)),
            size(collect_set(c_string,1)),
            size(collect_set(c_date,1)),
            size(collect_set(c_datev2,2)),
            size(collect_set(c_date_time,1)),
            size(collect_set(c_date_timev2,1)),
            size(collect_set(c_string_not_null,1))
        FROM
            ${tableName}
    """

    order_qt_select """
        SELECT
            size(collect_list(c_bool,1)),
            size(collect_list(c_tinyint,1)),
            size(collect_list(c_smallint,1)),
            size(collect_list(c_int,1)),
            size(collect_list(c_bigint,1)),
            size(collect_list(c_largeint,3)),
            size(collect_list(c_float,1)),
            size(collect_list(c_double,2)),
            size(collect_list(c_decimal,1)),
            size(collect_list(c_char,1)),
            size(collect_list(c_varchar,1)),
            size(collect_list(c_string,1)),
            size(collect_list(c_date,1)),
            size(collect_list(c_datev2,2)),
            size(collect_list(c_date_time,1)),
            size(collect_list(c_date_timev2,1)),
            size(collect_list(c_string_not_null,1))
        FROM
            ${tableName}
    """

    sql """
        CREATE TABLE ${tableCTAS1} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            collect_set(c_bool),
            collect_set(c_tinyint),
            collect_set(c_smallint),
            collect_set(c_int),
            collect_set(c_bigint),
            collect_set(c_largeint),
            collect_set(c_float),
            collect_set(c_double),
            collect_set(c_decimal),
            collect_set(c_char),
            collect_set(c_varchar),
            collect_set(c_string),
            collect_set(c_date),
            collect_set(c_datev2),
            collect_set(c_date_time),
            collect_set(c_date_timev2),
            collect_set(c_string_not_null)
        FROM
            ${tableName}
    """


    sql """
        CREATE TABLE ${tableCTAS2} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            size(collect_set(c_bool,1)),
            size(collect_set(c_tinyint,1)),
            size(collect_set(c_smallint,1)),
            size(collect_set(c_int,1)),
            size(collect_set(c_bigint,1)),
            size(collect_set(c_largeint,3)),
            size(collect_set(c_float,1)),
            size(collect_set(c_double,2)),
            size(collect_set(c_decimal,1)),
            size(collect_set(c_char,1)),
            size(collect_set(c_varchar,1)),
            size(collect_set(c_string,1)),
            size(collect_set(c_date,1)),
            size(collect_set(c_datev2,2)),
            size(collect_set(c_date_time,1)),
            size(collect_set(c_date_timev2,1)),
            size(collect_set(c_string_not_null,1))
        FROM
            ${tableName}
    """

    sql """
        CREATE TABLE ${tableCTAS3} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            collect_list(c_bool),
            collect_list(c_tinyint),
            collect_list(c_smallint),
            collect_list(c_int),
            collect_list(c_bigint),
            collect_list(c_largeint),
            collect_list(c_float),
            collect_list(c_double),
            collect_list(c_decimal),
            collect_list(c_char),
            collect_list(c_varchar),
            collect_list(c_string),
            collect_list(c_date),
            collect_list(c_datev2),
            collect_list(c_date_time),
            collect_list(c_date_timev2),
            collect_list(c_string_not_null)
        FROM
            ${tableName}
    """

    sql """
        CREATE TABLE ${tableCTAS4} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            size(collect_list(c_bool,1)),
            size(collect_list(c_tinyint,1)),
            size(collect_list(c_smallint,1)),
            size(collect_list(c_int,1)),
            size(collect_list(c_bigint,1)),
            size(collect_list(c_largeint,3)),
            size(collect_list(c_float,1)),
            size(collect_list(c_double,2)),
            size(collect_list(c_decimal,1)),
            size(collect_list(c_char,1)),
            size(collect_list(c_varchar,1)),
            size(collect_list(c_string,1)),
            size(collect_list(c_date,1)),
            size(collect_list(c_datev2,2)),
            size(collect_list(c_date_time,1)),
            size(collect_list(c_date_timev2,1)),
            size(collect_list(c_string_not_null,1))
        FROM
            ${tableName}
    """

    sql "SELECT * FROM ${tableCTAS1}"
    sql "SELECT * FROM ${tableCTAS2}"
    sql "SELECT * FROM ${tableCTAS3}"
    sql "SELECT * FROM ${tableCTAS4}"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2}"
    sql "DROP TABLE IF EXISTS ${tableCTAS3}"
    sql "DROP TABLE IF EXISTS ${tableCTAS4}"

    def tableName_11 = "group_uniq_array_test"
    def tableCTAS1_11 = "group_uniq_array_test_ctas1"
    def tableCTAS2_11 = "group_uniq_array_test_ctas2"
    def tableCTAS3_11 = "group_array_test_ctas3"
    def tableCTAS4_11 = "group_array_test_ctas4"

    sql "DROP TABLE IF EXISTS ${tableName_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS3_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS4_11}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName_11} (
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
        INSERT INTO ${tableName_11} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (1, false, 10, 20, 30, 4444444444444, 55555555555, 0.1, 0.222, 3333.33, 'c', 'varchar1', 'string1',
            '2022-12-01', '2022-12-01', '2022-12-01 22:23:23', '2022-12-01 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName_11} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (1, false, 11, 21, 33, 4444444444444, 55555555555, 0.1, 0.222, 3333.33, 'c', 'varchar1', 'string1',
            '2022-12-01', '2022-12-01', '2022-12-01 22:23:23', '2022-12-01 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName_11} values
            (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (1, true, 11, 12, 13, 1444444444444, 1555555555, 1.1, 1.222, 13333.33, 'd', 'varchar2', 'string2',
            '2022-12-02', '2022-12-02', '2022-12-02 22:23:23', '2022-12-02 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName_11} values
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (2, false, 21, 22, 23, 2444444444444, 255555555, 2.1, 2.222, 23333.33, 'f', 'varchar3', 'string3',
            '2022-12-03', '2022-12-03', '2022-12-03 22:23:23', '2022-12-03 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName_11} values
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (2, true, 31, 32, 33, 3444444444444, 3555555555, 3.1, 3.222, 33333.33, 'l', 'varchar3', 'string3',
            '2022-12-03', '2022-12-03', '2022-12-03 22:23:23', '2022-12-03 22:23:24.999999', 'not null')
    """

    sql """
        INSERT INTO ${tableName_11} values
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 'not null'),
            (2, false, 10, 20, 30, 944444444444, 9555555555, 9.1, 9.222, 93333.33, 'p', 'varchar9', 'string9',
            '2022-12-09', '2022-12-09', '2022-12-09 22:23:23', '2022-12-09 22:23:24.999999', 'not null')
    """

    qt_select """
        SELECT * FROM ${tableName_11} ORDER BY c_id, c_tinyint, c_char;
    """

    sql """
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
            ${tableName_11}
    """

    sql """
        SELECT
            group_array(c_bool),
            group_array(c_tinyint),
            group_array(c_smallint),
            group_array(c_int),
            group_array(c_bigint),
            group_array(c_largeint),
            group_array(c_float),
            group_array(c_double),
            group_array(c_decimal),
            group_array(c_char),
            group_array(c_varchar),
            group_array(c_string),
            group_array(c_date),
            group_array(c_datev2),
            group_array(c_date_time),
            group_array(c_date_timev2),
            group_array(c_string_not_null)
        FROM
            ${tableName_11}
    """

    order_qt_select """
        SELECT
            size(group_uniq_array(c_bool,1)),
            size(group_uniq_array(c_tinyint,1)),
            size(group_uniq_array(c_smallint,1)),
            size(group_uniq_array(c_int,1)),
            size(group_uniq_array(c_bigint,1)),
            size(group_uniq_array(c_largeint,3)),
            size(group_uniq_array(c_float,1)),
            size(group_uniq_array(c_double,2)),
            size(group_uniq_array(c_decimal,1)),
            size(group_uniq_array(c_char,1)),
            size(group_uniq_array(c_varchar,1)),
            size(group_uniq_array(c_string,1)),
            size(group_uniq_array(c_date,1)),
            size(group_uniq_array(c_datev2,2)),
            size(group_uniq_array(c_date_time,1)),
            size(group_uniq_array(c_date_timev2,1)),
            size(group_uniq_array(c_string_not_null,1))
        FROM
            ${tableName_11}
    """

    order_qt_select """
        SELECT
            size(group_array(c_bool,1)),
            size(group_array(c_tinyint,1)),
            size(group_array(c_smallint,1)),
            size(group_array(c_int,1)),
            size(group_array(c_bigint,1)),
            size(group_array(c_largeint,3)),
            size(group_array(c_float,1)),
            size(group_array(c_double,2)),
            size(group_array(c_decimal,1)),
            size(group_array(c_char,1)),
            size(group_array(c_varchar,1)),
            size(group_array(c_string,1)),
            size(group_array(c_date,1)),
            size(group_array(c_datev2,2)),
            size(group_array(c_date_time,1)),
            size(group_array(c_date_timev2,1)),
            size(group_array(c_string_not_null,1))
        FROM
            ${tableName_11}
    """

    sql """
        CREATE TABLE ${tableCTAS1_11} PROPERTIES("replication_num" = "1") AS
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
            ${tableName_11}
    """

    sql """
        CREATE TABLE ${tableCTAS2_11} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            size(group_uniq_array(c_bool,1)),
            size(group_uniq_array(c_tinyint,1)),
            size(group_uniq_array(c_smallint,1)),
            size(group_uniq_array(c_int,1)),
            size(group_uniq_array(c_bigint,1)),
            size(group_uniq_array(c_largeint,3)),
            size(group_uniq_array(c_float,1)),
            size(group_uniq_array(c_double,2)),
            size(group_uniq_array(c_decimal,1)),
            size(group_uniq_array(c_char,1)),
            size(group_uniq_array(c_varchar,1)),
            size(group_uniq_array(c_string,1)),
            size(group_uniq_array(c_date,1)),
            size(group_uniq_array(c_datev2,2)),
            size(group_uniq_array(c_date_time,1)),
            size(group_uniq_array(c_date_timev2,1)),
            size(group_uniq_array(c_string_not_null,1))
        FROM
            ${tableName_11}
    """

    sql """
        CREATE TABLE ${tableCTAS3_11} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            group_array(c_bool),
            group_array(c_tinyint),
            group_array(c_smallint),
            group_array(c_int),
            group_array(c_bigint),
            group_array(c_largeint),
            group_array(c_float),
            group_array(c_double),
            group_array(c_decimal),
            group_array(c_char),
            group_array(c_varchar),
            group_array(c_string),
            group_array(c_date),
            group_array(c_datev2),
            group_array(c_date_time),
            group_array(c_date_timev2),
            group_array(c_string_not_null)
        FROM
            ${tableName_11}
    """

    sql """
        CREATE TABLE ${tableCTAS4_11} PROPERTIES("replication_num" = "1") AS
        SELECT
	    1,
            size(group_array(c_bool,1)),
            size(group_array(c_tinyint,1)),
            size(group_array(c_smallint,1)),
            size(group_array(c_int,1)),
            size(group_array(c_bigint,1)),
            size(group_array(c_largeint,3)),
            size(group_array(c_float,1)),
            size(group_array(c_double,2)),
            size(group_array(c_decimal,1)),
            size(group_array(c_char,1)),
            size(group_array(c_varchar,1)),
            size(group_array(c_string,1)),
            size(group_array(c_date,1)),
            size(group_array(c_datev2,2)),
            size(group_array(c_date_time,1)),
            size(group_array(c_date_timev2,1)),
            size(group_array(c_string_not_null,1))
        FROM
            ${tableName_11}
    """

    sql "SELECT * FROM ${tableCTAS1_11}"
    sql "SELECT * FROM ${tableCTAS2_11}"
    sql "SELECT * FROM ${tableCTAS3_11}"
    sql "SELECT * FROM ${tableCTAS4_11}"

    sql "DROP TABLE IF EXISTS ${tableName_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS1_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS2_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS3_11}"
    sql "DROP TABLE IF EXISTS ${tableCTAS4_11}"

    // topn_array
    def tableName_12 = "topn_array"

    sql "DROP TABLE IF EXISTS ${tableName_12}"
    sql """
        CREATE TABLE IF NOT EXISTS topn_array (
            id int,
	        level int,
            dt datev2,
            num decimal(27,9)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO topn_array values(1,10,'2022-11-1',6.8754576), (2,8,'2022-11-3',0.576), (2,10,'2022-11-2',1.234) ,(3,10,'2022-11-2',0.576) ,(5,29,'2022-11-2',6.8754576) ,(6,8,'2022-11-1',6.8754576)"

    order_qt_select43 "select topn_array(level,2) from ${tableName_12}"
    order_qt_select44 "select topn_array(level,2,100) from ${tableName_12}"
    order_qt_select45 "select topn_array(dt,2,100) from ${tableName_12}"
    order_qt_select46 "select topn_array(num,2,100) from ${tableName_12}"
    sql "DROP TABLE IF EXISTS ${tableName_12}"
}
