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

suite("test_scalar_types_load", "p0") {

    def dataFile = """${getS3Url()}/regression/datatypes/test_scalar_types_10w.csv"""

    // define dup key table1
    def testTable = "tbl_scalar_types_dup"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
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
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "store_row_column" = "true");
        """

    // load data
    streamLoad {
        table testTable
        file dataFile
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(100000, json.NumberTotalRows)
            assertEquals(100000, json.NumberLoadedRows)
        }
    }

    // insert two NULL rows
    sql """INSERT INTO ${testTable} VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"""
    sql """INSERT INTO ${testTable} VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"""

    // define dup key table2 with 3 keys
    testTable = "tbl_scalar_types_dup_3keys"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `c_datetimev2` datetimev2(0) NULL,
            `c_bigint` bigint(20) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c_datetimev2`, `c_bigint`, `c_decimalv3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "store_row_column" = "true");
        """

    // insert data into dup key table2 3 times
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""


    // define unique key table1 enable mow
    testTable = "tbl_scalar_types_unique1"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `c_datetimev2` datetimev2(0) NULL,
            `c_bigint` bigint(20) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`c_datetimev2`, `c_bigint`, `c_decimalv3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true", "store_row_column" = "true");
        """

    // insert data into unique key table1 2 times
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""


    // define unique key table2 disable mow
    testTable = "tbl_scalar_types_unique2"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `c_datetimev2` datetimev2(0) NULL,
            `c_bigint` bigint(20) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`c_datetimev2`, `c_bigint`, `c_decimalv3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "false");
        """

    // insert data into unique key table1 2 times
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""


    // define dup key table with index
    testTable = "tbl_scalar_types_dup_bitmapindex"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
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
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL,
            INDEX idx_c_bool (c_bool) USING BITMAP,
            INDEX idx_c_tinyint (c_tinyint) USING BITMAP,
            INDEX idx_c_smallint (c_smallint) USING BITMAP,
            INDEX idx_c_int (c_int) USING BITMAP,
            INDEX idx_c_bigint (c_bigint) USING BITMAP,
            INDEX idx_c_largeint (c_largeint) USING BITMAP,
            INDEX idx_c_decimal (c_decimal) USING BITMAP,
            INDEX idx_c_decimalv3 (c_decimalv3) USING BITMAP,
            INDEX idx_c_date (c_date) USING BITMAP,
            INDEX idx_c_datetime (c_datetime) USING BITMAP,
            INDEX idx_c_datev2 (c_datev2) USING BITMAP,
            INDEX idx_c_datetimev2 (c_datetimev2) USING BITMAP,
            INDEX idx_c_char (c_char) USING BITMAP,
            INDEX idx_c_varchar (c_varchar) USING BITMAP,
            INDEX idx_c_string (c_string) USING BITMAP
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "store_row_column" = "true");
        """

    // insert data into dup table with index
    sql """INSERT INTO ${testTable} SELECT * FROM tbl_scalar_types_dup"""



    // define unique key table1 enable mow
    testTable = "tbl_scalar_types_unique1_bitmapindex"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `c_datetimev2` datetimev2(0) NULL,
            `c_bigint` bigint(20) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL,
            INDEX idx_c_bool (c_bool) USING BITMAP,
            INDEX idx_c_tinyint (c_tinyint) USING BITMAP,
            INDEX idx_c_smallint (c_smallint) USING BITMAP,
            INDEX idx_c_int (c_int) USING BITMAP,
            INDEX idx_c_bigint (c_bigint) USING BITMAP,
            INDEX idx_c_largeint (c_largeint) USING BITMAP,
            INDEX idx_c_decimal (c_decimal) USING BITMAP,
            INDEX idx_c_decimalv3 (c_decimalv3) USING BITMAP,
            INDEX idx_c_date (c_date) USING BITMAP,
            INDEX idx_c_datetime (c_datetime) USING BITMAP,
            INDEX idx_c_datev2 (c_datev2) USING BITMAP,
            INDEX idx_c_datetimev2 (c_datetimev2) USING BITMAP,
            INDEX idx_c_char (c_char) USING BITMAP,
            INDEX idx_c_varchar (c_varchar) USING BITMAP,
            INDEX idx_c_string (c_string) USING BITMAP
        ) ENGINE=OLAP
        UNIQUE KEY(`c_datetimev2`, `c_bigint`, `c_decimalv3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    // insert data into unique key table1 2 times
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""

    // define unique key table2 enable mow
    testTable = "tbl_scalar_types_unique2_bitmapindex"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `c_datetimev2` datetimev2(0) NULL,
            `c_bigint` bigint(20) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_date` date NULL,
            `c_datetime` datetime NULL,
            `c_datev2` datev2 NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL,
            INDEX idx_c_bool (c_bool) USING BITMAP,
            INDEX idx_c_tinyint (c_tinyint) USING BITMAP,
            INDEX idx_c_smallint (c_smallint) USING BITMAP,
            INDEX idx_c_int (c_int) USING BITMAP,
            INDEX idx_c_bigint (c_bigint) USING BITMAP,
            INDEX idx_c_largeint (c_largeint) USING BITMAP,
            INDEX idx_c_decimal (c_decimal) USING BITMAP,
            INDEX idx_c_decimalv3 (c_decimalv3) USING BITMAP,
            INDEX idx_c_date (c_date) USING BITMAP,
            INDEX idx_c_datetime (c_datetime) USING BITMAP,
            INDEX idx_c_datev2 (c_datev2) USING BITMAP,
            INDEX idx_c_datetimev2 (c_datetimev2) USING BITMAP,
            INDEX idx_c_char (c_char) USING BITMAP,
            INDEX idx_c_varchar (c_varchar) USING BITMAP,
            INDEX idx_c_string (c_string) USING BITMAP
        ) ENGINE=OLAP
        UNIQUE KEY(`c_datetimev2`, `c_bigint`, `c_decimalv3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "false");
        """

    // insert data into unique key table2 2 times
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""


    // define dup key table with index
    testTable = "tbl_scalar_types_dup_inverted_index"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
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
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL,
            INDEX idx_c_bool (c_bool) USING INVERTED,
            INDEX idx_c_tinyint (c_tinyint) USING INVERTED,
            INDEX idx_c_smallint (c_smallint) USING INVERTED,
            INDEX idx_c_int (c_int) USING INVERTED,
            INDEX idx_c_bigint (c_bigint) USING INVERTED,
            INDEX idx_c_largeint (c_largeint) USING INVERTED,
            INDEX idx_c_decimal (c_decimal) USING INVERTED,
            INDEX idx_c_decimalv3 (c_decimalv3) USING INVERTED,
            INDEX idx_c_date (c_date) USING INVERTED,
            INDEX idx_c_datetime (c_datetime) USING INVERTED,
            INDEX idx_c_datev2 (c_datev2) USING INVERTED,
            INDEX idx_c_datetimev2 (c_datetimev2) USING INVERTED,
            INDEX idx_c_char (c_char) USING INVERTED,
            INDEX idx_c_varchar (c_varchar) USING INVERTED,
            INDEX idx_c_string (c_string) USING INVERTED PROPERTIES("parser"="english")
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "store_row_column" = "true");
        """

    // insert data into dup table with index
    sql """INSERT INTO ${testTable} SELECT * FROM tbl_scalar_types_dup"""


// define dup key table with index
    testTable = "tbl_scalar_types_dup_inverted_index_skip"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
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
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_datetimev2` datetimev2(0) NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL,
            INDEX idx_c_bool (c_bool) USING INVERTED,
            INDEX idx_c_tinyint (c_tinyint) USING INVERTED,
            INDEX idx_c_smallint (c_smallint) USING INVERTED,
            INDEX idx_c_int (c_int) USING INVERTED,
            INDEX idx_c_bigint (c_bigint) USING INVERTED,
            INDEX idx_c_largeint (c_largeint) USING INVERTED,
            INDEX idx_c_decimal (c_decimal) USING INVERTED,
            INDEX idx_c_decimalv3 (c_decimalv3) USING INVERTED,
            INDEX idx_c_date (c_date) USING INVERTED,
            INDEX idx_c_datetime (c_datetime) USING INVERTED,
            INDEX idx_c_datev2 (c_datev2) USING INVERTED,
            INDEX idx_c_datetimev2 (c_datetimev2) USING INVERTED,
            INDEX idx_c_char (c_char) USING INVERTED,
            INDEX idx_c_varchar (c_varchar) USING INVERTED,
            INDEX idx_c_string (c_string) USING INVERTED PROPERTIES("parser"="english")
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "skip_write_index_on_load" = "false");
        """

    // insert data into dup table with index
    sql """INSERT INTO ${testTable} SELECT * FROM tbl_scalar_types_dup"""


    // define unique key table1 enable mow
    testTable = "tbl_scalar_types_unique1_inverted_index"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `c_datetimev2` datetimev2(0) NULL,
            `c_bigint` bigint(20) NULL,
            `c_decimalv3` decimalv3(20, 3) NULL,
            `c_bool` boolean NULL,
            `c_tinyint` tinyint(4) NULL,
            `c_smallint` smallint(6) NULL,
            `c_int` int(11) NULL,
            `c_largeint` largeint(40) NULL,
            `c_float` float NULL,
            `c_double` double NULL,
            `c_decimal` decimal(20, 3) NULL,
            `c_date` datev1 NULL,
            `c_datetime` datetimev1 NULL,
            `c_datev2` datev2 NULL,
            `c_char` char(15) NULL,
            `c_varchar` varchar(100) NULL,
            `c_string` text NULL,
            INDEX idx_c_bool (c_bool) USING INVERTED,
            INDEX idx_c_tinyint (c_tinyint) USING INVERTED,
            INDEX idx_c_smallint (c_smallint) USING INVERTED,
            INDEX idx_c_int (c_int) USING INVERTED,
            INDEX idx_c_bigint (c_bigint) USING INVERTED,
            INDEX idx_c_largeint (c_largeint) USING INVERTED,
            INDEX idx_c_decimal (c_decimal) USING INVERTED,
            INDEX idx_c_decimalv3 (c_decimalv3) USING INVERTED,
            INDEX idx_c_date (c_date) USING INVERTED,
            INDEX idx_c_datetime (c_datetime) USING INVERTED,
            INDEX idx_c_datev2 (c_datev2) USING INVERTED,
            INDEX idx_c_datetimev2 (c_datetimev2) USING INVERTED,
            INDEX idx_c_char (c_char) USING INVERTED,
            INDEX idx_c_varchar (c_varchar) USING INVERTED,
            INDEX idx_c_string (c_string) USING INVERTED PROPERTIES("parser"="english")
        ) ENGINE=OLAP
        UNIQUE KEY(`c_datetimev2`, `c_bigint`, `c_decimalv3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c_bigint`) BUCKETS 10
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    // insert data into unique key table1 2 times
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
    sql """INSERT INTO ${testTable} SELECT `c_datetimev2`, `c_bigint`, `c_decimalv3`,
            `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_largeint`,
            `c_float`, `c_double`, `c_decimal`, `c_date`, `c_datetime`, `c_datev2`,
            `c_char`, `c_varchar`, `c_string` FROM tbl_scalar_types_dup"""
}
