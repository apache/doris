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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_nested_types_insert_into_with_duplicat_table", "p0") {
    sql """ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false')"""
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

    // test action for scala to array with scala type
    //  current we support char family to insert nested type
    test {
        sql "insert into ${nested_table_dup} (c_bool) select c_bool from ${scala_table_dup}"
        exception "java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=ARRAY<BOOLEAN>"
    }

    test {
        sql "insert into ${nested_table_dup} (c_tinyint) select c_tinyint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=ARRAY<TINYINT(4)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_smallint) select c_smallint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=ARRAY<SMALLINT(6)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_int) select c_int from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=ARRAY<INT(11)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_largeint) select c_largeint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=ARRAY<LARGEINT(40)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_float) select c_float from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=ARRAY<FLOAT>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_double) select c_double from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=ARRAY<DOUBLE>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_decimal) select c_decimal from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_decimalv3) select c_decimalv3 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_date) select c_date from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<DATEV2>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_datetime) select c_datetime from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<DATETIMEV2(0)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_datev2) select c_datev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<DATEV2>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_datetimev2) select c_datetimev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<DATETIMEV2(0)>")
    }

    test {
        sql "insert into ${nested_table_dup} (c_char) select c_char from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_dup} (c_varchar) select c_varchar from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_dup} (c_string) select c_string from ${scala_table_dup}"
        exception null
    }

    qt_sql_nested_table_dup_c """select count() from ${nested_table_dup};"""

    // test action for scala to array with array-scala type
    test {
        sql "insert into ${nested_table_dup2} (c_bool) select c_bool from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=ARRAY<ARRAY<BOOLEAN>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_tinyint) select c_tinyint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=ARRAY<ARRAY<TINYINT(4)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_smallint) select c_smallint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=ARRAY<ARRAY<SMALLINT(6)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_int) select c_int from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=ARRAY<ARRAY<INT(11)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_largeint) select c_largeint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=ARRAY<ARRAY<LARGEINT(40)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_float) select c_float from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=ARRAY<ARRAY<FLOAT>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_double) select c_double from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=ARRAY<ARRAY<DOUBLE>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_decimal) select c_decimal from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_decimalv3) select c_decimalv3 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_date) select c_date from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<ARRAY<DATEV2>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_datetime) select c_datetime from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_datev2) select c_datev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<ARRAY<DATEV2>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_datetimev2) select c_datetimev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into ${nested_table_dup2} (c_char) select c_char from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_dup2} (c_varchar) select c_varchar from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_dup2} (c_string) select c_string from ${scala_table_dup}"
        exception null
    }

    qt_sql_nested_table_dup2_c """select count() from ${nested_table_dup2};"""


    // test action for scala to map with map-scala-scala type
    test {
        sql "insert into ${nested_table_map_dup} (c_bool) select c_bool from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=MAP<BOOLEAN,BOOLEAN>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_tinyint) select c_tinyint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=MAP<TINYINT(4),TINYINT(4)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_smallint) select c_smallint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=MAP<SMALLINT(6),SMALLINT(6)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_int) select c_int from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=MAP<INT(11),INT(11)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_largeint) select c_largeint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=MAP<LARGEINT(40),LARGEINT(40)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_float) select c_float from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=MAP<FLOAT,FLOAT>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_double) select c_double from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=MAP<DOUBLE,DOUBLE>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_decimal) select c_decimal from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_decimalv3) select c_decimalv3 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_date) select c_date from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,DATEV2>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_datetime) select c_datetime from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),DATETIMEV2(0)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_datev2) select c_datev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,DATEV2>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_datetimev2) select c_datetimev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),DATETIMEV2(0)>")
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_char) select c_char from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_varchar) select c_varchar from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_map_dup} (c_string) select c_string from ${scala_table_dup}"
        exception null
    }

    qt_sql_nested_table_map_dup_c """select count() from ${nested_table_map_dup};"""

    // test action for scala to array with map-scala-scala type
    test {
        sql "insert into ${nested_table_array_map_dup} (c_bool) select c_bool from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=ARRAY<MAP<BOOLEAN,BOOLEAN>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_tinyint) select c_tinyint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=ARRAY<MAP<TINYINT(4),TINYINT(4)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_smallint) select c_smallint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=ARRAY<MAP<SMALLINT(6),SMALLINT(6)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_int) select c_int from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=ARRAY<MAP<INT(11),INT(11)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_largeint) select c_largeint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=ARRAY<MAP<LARGEINT(40),LARGEINT(40)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_float) select c_float from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=ARRAY<MAP<FLOAT,FLOAT>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_double) select c_double from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=ARRAY<MAP<DOUBLE,DOUBLE>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_decimal) select c_decimal from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_decimalv3) select c_decimalv3 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_date) select c_date from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<MAP<DATEV2,DATEV2>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_datetime) select c_datetime from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<MAP<DATETIMEV2(0),DATETIMEV2(0)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_datev2) select c_datev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<MAP<DATEV2,DATEV2>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_datetimev2) select c_datetimev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<MAP<DATETIMEV2(0),DATETIMEV2(0)>>")
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_char) select c_char from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_varchar) select c_varchar from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_array_map_dup} (c_string) select c_string from ${scala_table_dup}"
        exception null
    }

    qt_sql_nested_table_array_map_dup_c """select count() from ${nested_table_array_map_dup};"""

    // test action for map with scala array-scala
    // test action for scala to array with array-scala type
    test {
        sql "insert into ${nested_table_map_array_dup} (c_bool) select c_bool from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=MAP<BOOLEAN,ARRAY<BOOLEAN>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_tinyint) select c_tinyint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=MAP<TINYINT(4),ARRAY<TINYINT(4)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_smallint) select c_smallint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=MAP<SMALLINT(6),ARRAY<SMALLINT(6)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_int) select c_int from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=MAP<INT(11),ARRAY<INT(11)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_largeint) select c_largeint from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=MAP<LARGEINT(40),ARRAY<LARGEINT(40)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_float) select c_float from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=MAP<FLOAT,ARRAY<FLOAT>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_double) select c_double from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=MAP<DOUBLE,ARRAY<DOUBLE>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_decimal) select c_decimal from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_decimalv3) select c_decimalv3 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_date) select c_date from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,ARRAY<DATEV2>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_datetime) select c_datetime from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_datev2) select c_datev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,ARRAY<DATEV2>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_datetimev2) select c_datetimev2 from ${scala_table_dup}"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_char) select c_char from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_varchar) select c_varchar from ${scala_table_dup}"
        exception null
    }

    test {
        sql "insert into ${nested_table_map_array_dup} (c_string) select c_string from ${scala_table_dup}"
        exception null
    }

    qt_sql_nested_table_map_array_dup_c """select count() from ${nested_table_map_array_dup};"""

}
