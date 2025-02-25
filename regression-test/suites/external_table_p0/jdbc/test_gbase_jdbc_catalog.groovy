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

suite("test_gbase_jdbc_catalog", "p0,external,gbase,external_docker,external_docker_gbase") {
    // Because there is no Gbase Dcoker test environment, the test case will not be executed for the time being.
    // Gbase ddl
    // CREATE TABLE gbase_test (
    //     tinyint_col TINYINT,
    //     smallint_col SMALLINT,
    //     int_col INT,
    //     bigint_col BIGINT,
    //     float_col FLOAT,
    //     double_col DOUBLE,
    //     decimal_col DECIMAL(10, 2),
    //     numeric_col NUMERIC(10, 2),
    //     char_col CHAR(255),
    //     varchar_col VARCHAR(10922),
    //     text_col TEXT,
    //     date_col DATE,
    //     datetime_col DATETIME,
    //     time_col TIME,
    //     timestamp_col TIMESTAMP
    // );

    //     INSERT INTO gbase_test VALUES (
    //         1,              -- tinyint_col
    //         18,             -- smallint_col
    //         100,            -- int_col
    //         500000,         -- bigint_col
    //         1.75,           -- float_col
    //         70.5,           -- double_col
    //         12345.67,       -- decimal_col
    //         100.00,         -- numeric_col
    //         'John Doe',     -- char_col
    //         'A description',-- varchar_col
    //         'Detailed text data', -- text_col
    //         '2023-09-19',   -- date_col
    //         '2023-09-19 12:34:56', -- datetime_col
    //         '12:00:00',     -- time_col
    //         '2023-09-19 14:45:00'  -- timestamp_col
    //     );
    //
    //
    //     INSERT INTO gbase_test VALUES (
    //         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    //     );

    // INSERT INTO gbase_test VALUES (
    //     -127,                        -- tinyint_col (min value)
    //     -32767,                      -- smallint_col (min value)
    //     -2147483647,                 -- int_col (min value)
    //     -92233720368,                -- bigint_col (min value)
    //     -3.40E+38,                   -- float_col (min value)
    //     -1.7976931348623157E+308,    -- double_col (min value)
    //     -(1E+9 - 1)/(1E+2),          -- decimal_col (min value)
    //     -(1E+9 - 1)/(1E+2),          -- numeric_col (min value)
    //     '',                          -- char_col (empty string，min value)
    //     '',                          -- varchar_col (empty string，min value)
    //     '',                          -- text_col (empty string，min value)
    //     '0001-01-01',                -- date_col (min value)
    //     '0001-01-01 00:00:00.000000',-- datetime_col (min value)
    //     '-838:59:59',                -- time_col (min value)
    //     '1970-01-01 08:00:01'        -- timestamp_col (min value)
    // );

    // INSERT INTO gbase_test VALUES (
    //     127,                         -- tinyint_col (max value)
    //     32767,                       -- smallint_col (max value)
    //     2147483647,                  -- int_col (max value)
    //     92233720368547758,           -- bigint_col (max value)
    //     3.40E+38,                    -- float_col (max value)
    //     1.7976931348623157E+308,     -- double_col (max value)
    //     (1E+9 - 1)/(1E+2),           -- decimal_col (max value)
    //     (1E+9 - 1)/(1E+2),           -- numeric_col (max value)
    //     REPEAT('Z', 255),            -- char_col (max value 255 characters)
    //     REPEAT('A', 10922),          -- varchar_col (max value 10922 characters)
    //     REPEAT('A', 21845),          -- text_col (max value 21845 characters)
    //     '9999-12-31',                -- date_col (max value)
    //     '9999-12-31 23:59:59.999999',-- datetime_col (max value)
    //     '838:59:59',                 -- time_col (max value)
    //     '2038-01-01 00:59:59'        -- timestamp_col (max value)
    // );

    // CREATE TABLE "pt1" (
    //   a datetime DEFAULT NULL
    // ) PARTITION BY RANGE(dayofmonth(a))
    // (PARTITION p0 VALUES LESS THAN (10));
    //
    // CREATE TABLE "pt2" (
    //   a datetime DEFAULT NULL
    // )
    //  PARTITION BY LIST (time_to_sec(a))
    // (PARTITION p0 VALUES IN (3,5,6,9,17));
    //
    // CREATE TABLE "pt3" (
    //   "a" int(11) DEFAULT NULL
    // )
    // PARTITION BY HASH (abs(a));
    //
    // CREATE TABLE "pt4" (
    //   "a" varchar(100) DEFAULT NULL
    // ) ENGINE=EXPRESS DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace'
    //  PARTITION BY KEY (a)
    // PARTITIONS 1;

    // Doris Catalog
    // sql """
    // CREATE CATALOG `gbase` PROPERTIES (
    // "user" = "root",
    // "type" = "jdbc",
    // "password" = "",
    // "jdbc_url" = "jdbc:gbase://127.0.0.1:5258/doris_test",
    // "driver_url" = "gbase-connector-java-9.5.0.7-build1-bin.jar",
    // "driver_class" = "com.gbase.jdbc.Driver"
    // ); """

    //sql """switch gbase;"""
    //sql """use doris_test;"""
    //order_qt_sample_table_desc """ desc gbase_test; """
    //order_qt_sample_table_select  """ select * from gbase_test order by 1; """
    //order_qt_show_tables  """ show tables; """

    // explain {
    //     sql("select tinyint_col from gbase_test limit 2;")

    //     contains "QUERY: SELECT `tinyint_col` FROM `doris_test`.`gbase_test` LIMIT 2"
    // }

}
