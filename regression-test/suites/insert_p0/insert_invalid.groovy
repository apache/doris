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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.
suite("insert_invalid") {
    sql """ DROP TABLE IF EXISTS datatype_invalid_base; """
    sql """ 
    CREATE TABLE `datatype_invalid_base` (
        `timea` varchar(30) NULL,
        `creatr` varchar(30) NULL
    ) UNIQUE KEY(`timea`)
      DISTRIBUTED BY HASH(`timea`) BUCKETS 1
      PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into
        datatype_invalid_base
    values
        ("12345678908876643", "a"),
        ("1234567890887664643", "b"),
        ("123456789088766445456", "c");
    """

    sql """ DROP TABLE IF EXISTS datatype_invalid; """
    sql """
    CREATE TABLE `datatype_invalid` (`timea` bigint NOT NULL, `creatr` varchar(30) NULL)
        UNIQUE KEY(`timea`)
        DISTRIBUTED BY HASH(`timea`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    
    sql """ set enable_insert_strict=true; """

    // test insert select: out of range value
    test {
        sql """ insert into datatype_invalid select * from datatype_invalid_base;"""
        exception "Invalid value in strict mode"
    }

    // test insert select: invalid value
    sql """ DROP TABLE IF EXISTS datatype_invalid_base; """
    sql """ 
    CREATE TABLE `datatype_invalid_base` (
        `timea` varchar(30) NULL,
        `creatr` varchar(30) NULL
    ) UNIQUE KEY(`timea`)
      DISTRIBUTED BY HASH(`timea`) BUCKETS 1
      PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into
        datatype_invalid_base
    values
        ("a", "a");
    """
    test {
        sql """ insert into datatype_invalid select * from datatype_invalid_base;"""
        exception "Invalid value in strict mode"
    }

    // test insert select: invalid value
    sql """ DROP TABLE IF EXISTS datatype_invalid_base; """
    sql """ 
    CREATE TABLE `datatype_invalid_base` (
        `timea` varchar(30) NULL,
        `creatr` varchar(30) NULL
    ) UNIQUE KEY(`timea`)
      DISTRIBUTED BY HASH(`timea`) BUCKETS 1
      PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into
        datatype_invalid_base
    values
        (" ", "a");
    """
    test {
        sql """ insert into datatype_invalid select * from datatype_invalid_base;"""
        exception "Invalid value in strict mode"
    }

    // test insert select: null into not nullable
    sql """ DROP TABLE IF EXISTS datatype_invalid_base; """
    sql """ 
    CREATE TABLE `datatype_invalid_base` (
        `timea` varchar(30) NULL,
        `creatr` varchar(30) NULL
    ) UNIQUE KEY(`timea`)
      DISTRIBUTED BY HASH(`timea`) BUCKETS 1
      PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into
        datatype_invalid_base
    values
        (null, "a");
    """
    test {
        sql """ insert into datatype_invalid select * from datatype_invalid_base;"""
        exception "null value for not null column"
    }

    // test insert
    test {
        sql """ insert into datatype_invalid values("a", "a");"""
        exception "Invalid number format"
    }
    test {
        sql """ insert into datatype_invalid values(" ", "a");"""
        exception "Invalid number format"
    }
    test {
        sql """ insert into datatype_invalid values(123456789088766445456, "a");"""
        exception "Number out of range"
    }
    test {
        sql """ insert into datatype_invalid values(null, "a");"""
        exception "null value for not null column"
    }

    def csv_files=["datetype_invalid1.csv", "datetype_invalid2.csv"]

    // test stream load
    for (String csv_file in csv_files) {
        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table 'datatype_invalid'

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'

            set 'strict_mode', 'true'

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file "${csv_file}"

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.toLowerCase().contains("invalid value in strict mode"))
            }
        }
    }

    // insert null in non-strict mode
    sql """ set enable_insert_strict=false; """
    test {
        sql """ insert into datatype_invalid values(null, "a");"""
        exception "null value for not null column"
    }

}
