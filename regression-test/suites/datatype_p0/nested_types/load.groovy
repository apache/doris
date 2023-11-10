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

suite("load") {
    // ddl begin
    sql """ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false')"""
    sql """set enable_nereids_planner=false"""
    def dataFile = """test_scalar_types_100.csv"""

    // define dup key table1 with scala types
    def scala_table_dup = "tbl_scalar_types_dup"
    sql "DROP TABLE IF EXISTS ${scala_table_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${scala_table_dup} (
            `k1` bigint(11) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_bigint` bigint(20) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_date` date NULL,
            `c_datetime` datetime NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // load data
    streamLoad {
        table scala_table_dup
        file dataFile
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(100, json.NumberTotalRows)
            assertEquals(100, json.NumberLoadedRows)
        }
    }

    // insert two NULL rows
    sql """INSERT INTO ${scala_table_dup} VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"""
    sql """INSERT INTO ${scala_table_dup} VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"""


    // define dup key table with nested table types with one nested scala
    def nested_table_dup = "tbl_array_nested_types_dup"
    sql "DROP TABLE IF EXISTS ${nested_table_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_dup} (
            `k1` bigint(11) NULL,
            `c_bool` array<boolean> NULL,
            `c_tinyint` array<tinyint(4)> NULL,
            `c_smallint` array<smallint(6)> NULL,
            `c_int` array<int(11)> NULL,
            `c_bigint` array<bigint(20)> NULL,
            `c_largeint` array<largeint(40)> NULL,
            `c_float` array<float> NULL,
            `c_double` array<double> NULL,
            `c_decimal` array<decimal(20, 3)> NULL,
            `c_decimalv3` array<decimalv3(20, 3)> NULL,
            `c_date` array<date> NULL,
            `c_datetime` array<datetime> NULL,
            `c_datev2` array<datev2> NULL,
            `c_datetimev2` array<datetimev2(0)> NULL,
            `c_char` array<char(15)> NULL,
            `c_varchar` array<varchar(100)> NULL,
            `c_string` array<text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define dup key table with nested table types with two nested scala
    def nested_table_dup2 = "tbl_array_nested_types_dup2"
    sql "DROP TABLE IF EXISTS ${nested_table_dup2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_dup2} (
            `k1` bigint(11) NULL,
            `c_bool` array<array<boolean>> NULL,
            `c_tinyint` array<array<tinyint(4)>> NULL,
            `c_smallint` array<array<smallint(6)>> NULL,
            `c_int` array<array<int(11)>> NULL,
            `c_bigint` array<array<bigint(20)>> NULL,
            `c_largeint` array<array<largeint(40)>> NULL,
            `c_float` array<array<float>> NULL,
            `c_double` array<array<double>> NULL,
            `c_decimal` array<array<decimal(20, 3)>> NULL,
            `c_decimalv3` array<array<decimalv3(20, 3)>> NULL,
            `c_date` array<array<date>> NULL,
            `c_datetime` array<array<datetime>> NULL,
            `c_datev2` array<array<datev2>> NULL,
            `c_datetimev2` array<array<datetimev2(0)>> NULL,
            `c_char` array<array<char(15)>> NULL,
            `c_varchar` array<array<varchar(100)>> NULL,
            `c_string` array<array<text>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define dup key table with map types with one nested scala
    def nested_table_map_dup = "tbl_map_types_dup"
    sql "DROP TABLE IF EXISTS ${nested_table_map_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_dup} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, boolean> NULL,
            `c_tinyint` map<tinyint(4), tinyint(4)> NULL,
            `c_smallint` map<smallint(6), smallint(6)> NULL,
            `c_int` map<int(11), int(11)> NULL,
            `c_bigint` map<bigint(20), bigint(20)> NULL,
            `c_largeint` map<largeint(40), largeint(40)> NULL,
            `c_float` map<float, float> NULL,
            `c_double` map<double, double> NULL,
            `c_decimal` map<decimal(20, 3), decimal(20, 3)> NULL,
            `c_decimalv3` map<decimalv3(20, 3), decimalv3(20, 3)> NULL,
            `c_date` map<date, date> NULL,
            `c_datetime` map<datetime, datetime> NULL,
            `c_datev2` map<datev2, datev2> NULL,
            `c_datetimev2` map<datetimev2(0), datetimev2(0)> NULL,
            `c_char` map<char(15), char(15)> NULL,
            `c_varchar` map<varchar(100), varchar(100)> NULL,
            `c_string` map<text, text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """


    // define dup key table with array nested map table types with one nested scala
    def nested_table_array_map_dup = "tbl_array_map_types_dup"
    sql "DROP TABLE IF EXISTS ${nested_table_array_map_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_array_map_dup} (
            `k1` bigint(11) NULL,
            `c_bool` array<map<boolean, boolean>> NULL,
            `c_tinyint` array<map<tinyint(4), tinyint(4)>> NULL,
            `c_smallint` array<map<smallint(6), smallint(6)>> NULL,
            `c_int` array<map<int(11), int(11)>> NULL,
            `c_bigint` array<map<bigint(20), bigint(20)>> NULL,
            `c_largeint` array<map<largeint(40), largeint(40)>> NULL,
            `c_float` array<map<float, float>> NULL,
            `c_double` array<map<double, double>> NULL,
            `c_decimal` array<map<decimal(20, 3), decimal(20, 3)>> NULL,
            `c_decimalv3` array<map<decimalv3(20, 3), decimalv3(20, 3)>> NULL,
            `c_date` array<map<date, date>> NULL,
            `c_datetime` array<map<datetime, datetime>> NULL,
            `c_datev2` array<map<datev2, datev2>> NULL,
            `c_datetimev2` array<map<datetimev2(0), datetimev2(0)>> NULL,
            `c_char` array<map<char(15), char(15)>> NULL,
            `c_varchar` array<map<varchar(100), varchar(100)>> NULL,
            `c_string` array<map<text, text>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define dup key table with map nested value array table types with one nested scala
    def nested_table_map_array_dup = "tbl_map_array_types_dup"
    sql "DROP TABLE IF EXISTS ${nested_table_map_array_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_array_dup} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, array<boolean>> NULL,
            `c_tinyint` map<tinyint(4), array<tinyint(4)>> NULL,
            `c_smallint` map<smallint(6), array<smallint(6)>> NULL,
            `c_int` map<int(11), array<int(11)>> NULL,
            `c_bigint` map<bigint(20), array<bigint(20)>> NULL,
            `c_largeint` map<largeint(40), array<largeint(40)>> NULL,
            `c_float` map<float, array<float>> NULL,
            `c_double` map<double, array<double>> NULL,
            `c_decimal` map<decimal(20, 3), array<decimal(20, 3)>> NULL,
            `c_decimalv3` map<decimalv3(20, 3), array<decimalv3(20, 3)>> NULL,
            `c_date` map<date, array<date>> NULL,
            `c_datetime` map<datetime, array<datetime>> NULL,
            `c_datev2` map<datev2, array<datev2>> NULL,
            `c_datetimev2` map<datetimev2(0), array<datetimev2(0)>> NULL,
            `c_char` map<char(15), array<char(15)>> NULL,
            `c_varchar` map<varchar(100), array<varchar(100)>> NULL,
            `c_string` map<text, array<text>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // unique table
    // define mor key table with nested table types with one nested scala
    def nested_table_mor = "tbl_array_nested_types_mor"
    sql "DROP TABLE IF EXISTS ${nested_table_mor}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_mor} (
            `k1` bigint(11) NULL,
            `c_bool` array<boolean> NULL,
            `c_tinyint` array<tinyint(4)> NULL,
            `c_smallint` array<smallint(6)> NULL,
            `c_int` array<int(11)> NULL,
            `c_bigint` array<bigint(20)> NULL,
            `c_largeint` array<largeint(40)> NULL,
            `c_float` array<float> NULL,
            `c_double` array<double> NULL,
            `c_decimal` array<decimal(20, 3)> NULL,
            `c_decimalv3` array<decimalv3(20, 3)> NULL,
            `c_date` array<date> NULL,
            `c_datetime` array<datetime> NULL,
            `c_datev2` array<datev2> NULL,
            `c_datetimev2` array<datetimev2(0)> NULL,
            `c_char` array<char(15)> NULL,
            `c_varchar` array<varchar(100)> NULL,
            `c_string` array<text> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define mor key table with nested table types with two nested scala
    def nested_table_mor2 = "tbl_array_nested_types_mor2"
    sql "DROP TABLE IF EXISTS ${nested_table_mor2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_mor2} (
            `k1` bigint(11) NULL,
            `c_bool` array<array<boolean>> NULL,
            `c_tinyint` array<array<tinyint(4)>> NULL,
            `c_smallint` array<array<smallint(6)>> NULL,
            `c_int` array<array<int(11)>> NULL,
            `c_bigint` array<array<bigint(20)>> NULL,
            `c_largeint` array<array<largeint(40)>> NULL,
            `c_float` array<array<float>> NULL,
            `c_double` array<array<double>> NULL,
            `c_decimal` array<array<decimal(20, 3)>> NULL,
            `c_decimalv3` array<array<decimalv3(20, 3)>> NULL,
            `c_date` array<array<date>> NULL,
            `c_datetime` array<array<datetime>> NULL,
            `c_datev2` array<array<datev2>> NULL,
            `c_datetimev2` array<array<datetimev2(0)>> NULL,
            `c_char` array<array<char(15)>> NULL,
            `c_varchar` array<array<varchar(100)>> NULL,
            `c_string` array<array<text>> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define mor key table with map types with one nested scala
    def nested_table_map_mor = "tbl_map_types_mor"
    sql "DROP TABLE IF EXISTS ${nested_table_map_mor}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_mor} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, boolean> NULL,
            `c_tinyint` map<tinyint(4), tinyint(4)> NULL,
            `c_smallint` map<smallint(6), smallint(6)> NULL,
            `c_int` map<int(11), int(11)> NULL,
            `c_bigint` map<bigint(20), bigint(20)> NULL,
            `c_largeint` map<largeint(40), largeint(40)> NULL,
            `c_float` map<float, float> NULL,
            `c_double` map<double, double> NULL,
            `c_decimal` map<decimal(20, 3), decimal(20, 3)> NULL,
            `c_decimalv3` map<decimalv3(20, 3), decimalv3(20, 3)> NULL,
            `c_date` map<date, date> NULL,
            `c_datetime` map<datetime, datetime> NULL,
            `c_datev2` map<datev2, datev2> NULL,
            `c_datetimev2` map<datetimev2(0), datetimev2(0)> NULL,
            `c_char` map<char(15), char(15)> NULL,
            `c_varchar` map<varchar(100), varchar(100)> NULL,
            `c_string` map<text, text> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """


    // define mor key table with array nested map table types with one nested scala
    def nested_table_array_map_mor = "tbl_array_map_types_mor"
    sql "DROP TABLE IF EXISTS ${nested_table_array_map_mor}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_array_map_mor} (
            `k1` bigint(11) NULL,
            `c_bool` array<map<boolean, boolean>> NULL,
            `c_tinyint` array<map<tinyint(4), tinyint(4)>> NULL,
            `c_smallint` array<map<smallint(6), smallint(6)>> NULL,
            `c_int` array<map<int(11), int(11)>> NULL,
            `c_bigint` array<map<bigint(20), bigint(20)>> NULL,
            `c_largeint` array<map<largeint(40), largeint(40)>> NULL,
            `c_float` array<map<float, float>> NULL,
            `c_double` array<map<double, double>> NULL,
            `c_decimal` array<map<decimal(20, 3), decimal(20, 3)>> NULL,
            `c_decimalv3` array<map<decimalv3(20, 3), decimalv3(20, 3)>> NULL,
            `c_date` array<map<date, date>> NULL,
            `c_datetime` array<map<datetime, datetime>> NULL,
            `c_datev2` array<map<datev2, datev2>> NULL,
            `c_datetimev2` array<map<datetimev2(0), datetimev2(0)>> NULL,
            `c_char` array<map<char(15), char(15)>> NULL,
            `c_varchar` array<map<varchar(100), varchar(100)>> NULL,
            `c_string` array<map<text, text>> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define mor key table with map nested value array table types with one nested scala
    def nested_table_map_array_mor = "tbl_map_array_types_mor"
    sql "DROP TABLE IF EXISTS ${nested_table_map_array_mor}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_array_mor} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, array<boolean>> NULL,
            `c_tinyint` map<tinyint(4), array<tinyint(4)>> NULL,
            `c_smallint` map<smallint(6), array<smallint(6)>> NULL,
            `c_int` map<int(11), array<int(11)>> NULL,
            `c_bigint` map<bigint(20), array<bigint(20)>> NULL,
            `c_largeint` map<largeint(40), array<largeint(40)>> NULL,
            `c_float` map<float, array<float>> NULL,
            `c_double` map<double, array<double>> NULL,
            `c_decimal` map<decimal(20, 3), array<decimal(20, 3)>> NULL,
            `c_decimalv3` map<decimalv3(20, 3), array<decimalv3(20, 3)>> NULL,
            `c_date` map<date, array<date>> NULL,
            `c_datetime` map<datetime, array<datetime>> NULL,
            `c_datev2` map<datev2, array<datev2>> NULL,
            `c_datetimev2` map<datetimev2(0), array<datetimev2(0)>> NULL,
            `c_char` map<char(15), array<char(15)>> NULL,
            `c_varchar` map<varchar(100), array<varchar(100)>> NULL,
            `c_string` map<text, array<text>> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define mow key table with nested table types with one nested scala
    def nested_table_mow = "tbl_array_nested_types_mow"
    sql "DROP TABLE IF EXISTS ${nested_table_mow}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_mow} (
            `k1` bigint(11) NULL,
            `c_bool` array<boolean> NULL,
            `c_tinyint` array<tinyint(4)> NULL,
            `c_smallint` array<smallint(6)> NULL,
            `c_int` array<int(11)> NULL,
            `c_bigint` array<bigint(20)> NULL,
            `c_largeint` array<largeint(40)> NULL,
            `c_float` array<float> NULL,
            `c_double` array<double> NULL,
            `c_decimal` array<decimal(20, 3)> NULL,
            `c_decimalv3` array<decimalv3(20, 3)> NULL,
            `c_date` array<date> NULL,
            `c_datetime` array<datetime> NULL,
            `c_datev2` array<datev2> NULL,
            `c_datetimev2` array<datetimev2(0)> NULL,
            `c_char` array<char(15)> NULL,
            `c_varchar` array<varchar(100)> NULL,
            `c_string` array<text> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    // define mow key table with nested table types with two nested scala
    def nested_table_mow2 = "tbl_array_nested_types_mow2"
    sql "DROP TABLE IF EXISTS ${nested_table_mow2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_mow2} (
            `k1` bigint(11) NULL,
            `c_bool` array<array<boolean>> NULL,
            `c_tinyint` array<array<tinyint(4)>> NULL,
            `c_smallint` array<array<smallint(6)>> NULL,
            `c_int` array<array<int(11)>> NULL,
            `c_bigint` array<array<bigint(20)>> NULL,
            `c_largeint` array<array<largeint(40)>> NULL,
            `c_float` array<array<float>> NULL,
            `c_double` array<array<double>> NULL,
            `c_decimal` array<array<decimal(20, 3)>> NULL,
            `c_decimalv3` array<array<decimalv3(20, 3)>> NULL,
            `c_date` array<array<date>> NULL,
            `c_datetime` array<array<datetime>> NULL,
            `c_datev2` array<array<datev2>> NULL,
            `c_datetimev2` array<array<datetimev2(0)>> NULL,
            `c_char` array<array<char(15)>> NULL,
            `c_varchar` array<array<varchar(100)>> NULL,
            `c_string` array<array<text>> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    // define mow key table with map types with one nested scala
    def nested_table_map_mow = "tbl_map_types_mow"
    sql "DROP TABLE IF EXISTS ${nested_table_map_mow}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_mow} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, boolean> NULL,
            `c_tinyint` map<tinyint(4), tinyint(4)> NULL,
            `c_smallint` map<smallint(6), smallint(6)> NULL,
            `c_int` map<int(11), int(11)> NULL,
            `c_bigint` map<bigint(20), bigint(20)> NULL,
            `c_largeint` map<largeint(40), largeint(40)> NULL,
            `c_float` map<float, float> NULL,
            `c_double` map<double, double> NULL,
            `c_decimal` map<decimal(20, 3), decimal(20, 3)> NULL,
            `c_decimalv3` map<decimalv3(20, 3), decimalv3(20, 3)> NULL,
            `c_date` map<date, date> NULL,
            `c_datetime` map<datetime, datetime> NULL,
            `c_datev2` map<datev2, datev2> NULL,
            `c_datetimev2` map<datetimev2(0), datetimev2(0)> NULL,
            `c_char` map<char(15), char(15)> NULL,
            `c_varchar` map<varchar(100), varchar(100)> NULL,
            `c_string` map<text, text> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """


    // define mow key table with array nested map table types with one nested scala
    def nested_table_array_map_mow = "tbl_array_map_types_mow"
    sql "DROP TABLE IF EXISTS ${nested_table_array_map_mow}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_array_map_mow} (
            `k1` bigint(11) NULL,
            `c_bool` array<map<boolean, boolean>> NULL,
            `c_tinyint` array<map<tinyint(4), tinyint(4)>> NULL,
            `c_smallint` array<map<smallint(6), smallint(6)>> NULL,
            `c_int` array<map<int(11), int(11)>> NULL,
            `c_bigint` array<map<bigint(20), bigint(20)>> NULL,
            `c_largeint` array<map<largeint(40), largeint(40)>> NULL,
            `c_float` array<map<float, float>> NULL,
            `c_double` array<map<double, double>> NULL,
            `c_decimal` array<map<decimal(20, 3), decimal(20, 3)>> NULL,
            `c_decimalv3` array<map<decimalv3(20, 3), decimalv3(20, 3)>> NULL,
            `c_date` array<map<date, date>> NULL,
            `c_datetime` array<map<datetime, datetime>> NULL,
            `c_datev2` array<map<datev2, datev2>> NULL,
            `c_datetimev2` array<map<datetimev2(0), datetimev2(0)>> NULL,
            `c_char` array<map<char(15), char(15)>> NULL,
            `c_varchar` array<map<varchar(100), varchar(100)>> NULL,
            `c_string` array<map<text, text>> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    // define dup key table with map nested value array table types with one nested scala
    def nested_table_map_array_mow = "tbl_map_array_types_mow"
    sql "DROP TABLE IF EXISTS ${nested_table_map_array_mow}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_array_mow} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, array<boolean>> NULL,
            `c_tinyint` map<tinyint(4), array<tinyint(4)>> NULL,
            `c_smallint` map<smallint(6), array<smallint(6)>> NULL,
            `c_int` map<int(11), array<int(11)>> NULL,
            `c_bigint` map<bigint(20), array<bigint(20)>> NULL,
            `c_largeint` map<largeint(40), array<largeint(40)>> NULL,
            `c_float` map<float, array<float>> NULL,
            `c_double` map<double, array<double>> NULL,
            `c_decimal` map<decimal(20, 3), array<decimal(20, 3)>> NULL,
            `c_decimalv3` map<decimalv3(20, 3), array<decimalv3(20, 3)>> NULL,
            `c_date` map<date, array<date>> NULL,
            `c_datetime` map<datetime, array<datetime>> NULL,
            `c_datev2` map<datev2, array<datev2>> NULL,
            `c_datetimev2` map<datetimev2(0), array<datetimev2(0)>> NULL,
            `c_char` map<char(15), array<char(15)>> NULL,
            `c_varchar` map<varchar(100), array<varchar(100)>> NULL,
            `c_string` map<text, array<text>> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    // agg table
    // define agg key table with nested table types with one nested scala
    def nested_table_agg = "tbl_array_nested_types_agg"
    sql "DROP TABLE IF EXISTS ${nested_table_agg}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_agg} (
            `k1` bigint(11) NULL,
            `c_bool` array<boolean> REPLACE,
            `c_tinyint` array<tinyint(4)> REPLACE,
            `c_smallint` array<smallint(6)> REPLACE,
            `c_int` array<int(11)> REPLACE,
            `c_bigint` array<bigint(20)> REPLACE,
            `c_largeint` array<largeint(40)> REPLACE,
            `c_float` array<float> REPLACE,
            `c_double` array<double> REPLACE,
            `c_decimal` array<decimal(20, 3)> REPLACE,
            `c_decimalv3` array<decimalv3(20, 3)> REPLACE,
            `c_date` array<date> REPLACE,
            `c_datetime` array<datetime> REPLACE,
            `c_datev2` array<datev2> REPLACE,
            `c_datetimev2` array<datetimev2(0)> REPLACE,
            `c_char` array<char(15)> REPLACE,
            `c_varchar` array<varchar(100)> REPLACE,
            `c_string` array<text> REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define agg key table with nested table types with two nested scala
    def nested_table_agg2 = "tbl_array_nested_types_agg2"
    sql "DROP TABLE IF EXISTS ${nested_table_agg2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_agg2} (
            `k1` bigint(11) NULL,
            `c_bool` array<array<boolean>> REPLACE,
            `c_tinyint` array<array<tinyint(4)>> REPLACE,
            `c_smallint` array<array<smallint(6)>> REPLACE,
            `c_int` array<array<int(11)>> REPLACE,
            `c_bigint` array<array<bigint(20)>> REPLACE,
            `c_largeint` array<array<largeint(40)>> REPLACE,
            `c_float` array<array<float>> REPLACE,
            `c_double` array<array<double>> REPLACE,
            `c_decimal` array<array<decimal(20, 3)>> REPLACE,
            `c_decimalv3` array<array<decimalv3(20, 3)>> REPLACE,
            `c_date` array<array<date>> REPLACE,
            `c_datetime` array<array<datetime>> REPLACE,
            `c_datev2` array<array<datev2>> REPLACE,
            `c_datetimev2` array<array<datetimev2(0)>> REPLACE,
            `c_char` array<array<char(15)>> REPLACE,
            `c_varchar` array<array<varchar(100)>> REPLACE,
            `c_string` array<array<text>> REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define mor key table with map types with one nested scala
    def nested_table_map_agg = "tbl_map_types_agg"
    sql "DROP TABLE IF EXISTS ${nested_table_map_agg}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_agg} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, boolean> REPLACE,
            `c_tinyint` map<tinyint(4), tinyint(4)> REPLACE,
            `c_smallint` map<smallint(6), smallint(6)> REPLACE,
            `c_int` map<int(11), int(11)> REPLACE,
            `c_bigint` map<bigint(20), bigint(20)> REPLACE,
            `c_largeint` map<largeint(40), largeint(40)> REPLACE,
            `c_float` map<float, float> REPLACE,
            `c_double` map<double, double> REPLACE,
            `c_decimal` map<decimal(20, 3), decimal(20, 3)> REPLACE,
            `c_decimalv3` map<decimalv3(20, 3), decimalv3(20, 3)> REPLACE,
            `c_date` map<date, date> REPLACE,
            `c_datetime` map<datetime, datetime> REPLACE,
            `c_datev2` map<datev2, datev2> REPLACE,
            `c_datetimev2` map<datetimev2(0), datetimev2(0)> REPLACE,
            `c_char` map<char(15), char(15)> REPLACE,
            `c_varchar` map<varchar(100), varchar(100)> REPLACE,
            `c_string` map<text, text> REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """


    // define agg key table with array nested map table types with one nested scala
    def nested_table_array_map_agg = "tbl_array_map_types_agg"
    sql "DROP TABLE IF EXISTS ${nested_table_array_map_agg}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_array_map_agg} (
            `k1` bigint(11) NULL,
            `c_bool` array<map<boolean, boolean>> REPLACE,
            `c_tinyint` array<map<tinyint(4), tinyint(4)>> REPLACE,
            `c_smallint` array<map<smallint(6), smallint(6)>> REPLACE,
            `c_int` array<map<int(11), int(11)>> REPLACE,
            `c_bigint` array<map<bigint(20), bigint(20)>> REPLACE,
            `c_largeint` array<map<largeint(40), largeint(40)>> REPLACE,
            `c_float` array<map<float, float>> REPLACE,
            `c_double` array<map<double, double>> REPLACE,
            `c_decimal` array<map<decimal(20, 3), decimal(20, 3)>> REPLACE,
            `c_decimalv3` array<map<decimalv3(20, 3), decimalv3(20, 3)>> REPLACE,
            `c_date` array<map<date, date>> REPLACE,
            `c_datetime` array<map<datetime, datetime>> REPLACE,
            `c_datev2` array<map<datev2, datev2>> REPLACE,
            `c_datetimev2` array<map<datetimev2(0), datetimev2(0)>> REPLACE,
            `c_char` array<map<char(15), char(15)>> REPLACE,
            `c_varchar` array<map<varchar(100), varchar(100)>> REPLACE,
            `c_string` array<map<text, text>> REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define agg key table with map nested value array table types with one nested scala
    def nested_table_map_array_agg = "tbl_map_array_types_agg"
    sql "DROP TABLE IF EXISTS ${nested_table_map_array_agg}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_array_agg} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, array<boolean>> REPLACE,
            `c_tinyint` map<tinyint(4), array<tinyint(4)>> REPLACE,
            `c_smallint` map<smallint(6), array<smallint(6)>> REPLACE,
            `c_int` map<int(11), array<int(11)>> REPLACE,
            `c_bigint` map<bigint(20), array<bigint(20)>> REPLACE,
            `c_largeint` map<largeint(40), array<largeint(40)>> REPLACE,
            `c_float` map<float, array<float>> REPLACE,
            `c_double` map<double, array<double>> REPLACE,
            `c_decimal` map<decimal(20, 3), array<decimal(20, 3)>> REPLACE,
            `c_decimalv3` map<decimalv3(20, 3), array<decimalv3(20, 3)>> REPLACE,
            `c_date` map<date, array<date>> REPLACE,
            `c_datetime` map<datetime, array<datetime>> REPLACE,
            `c_datev2` map<datev2, array<datev2>> REPLACE,
            `c_datetimev2` map<datetimev2(0), array<datetimev2(0)>> REPLACE,
            `c_char` map<char(15), array<char(15)>> REPLACE,
            `c_varchar` map<varchar(100), array<varchar(100)>> REPLACE,
            `c_string` map<text, array<text>> REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
}
