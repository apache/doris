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
    sql """ DROP TABLE IF EXISTS datatype_invalid; """
    sql """
    CREATE TABLE `datatype_invalid` (`timea` bigint NOT NULL, `creatr` varchar(30) NULL)
        UNIQUE KEY(`timea`)
        DISTRIBUTED BY HASH(`timea`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    
    // strict insert
    sql """ set enable_insert_strict=true; """

    // test insert select: out of range value
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

    test {
        sql """ insert into datatype_invalid select * from datatype_invalid_base;"""
        exception "Invalid value"
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
        exception "Invalid value"
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
        exception "Invalid value"
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
        exception "Insert has filtered data in strict mode"
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
        exception "Insert has filtered data in strict mode"
    }

    sql """ DROP TABLE IF EXISTS datatype_invalid; """
    sql """
    CREATE TABLE `datatype_invalid` (`timea` datetime NOT NULL, `creatr` varchar(30) NULL)
        UNIQUE KEY(`timea`)
        DISTRIBUTED BY HASH(`timea`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    test {
        sql """ insert into datatype_invalid values ('2022-02-29', 'a'); """
        exception "Invalid value"
    }

    sql """ set enable_insert_strict=false; """
    sql """ DROP TABLE IF EXISTS datatype_invalid; """
    sql """
    CREATE TABLE `datatype_invalid` (`timea` bigint NOT NULL, `creatr` varchar(30) NULL)
        UNIQUE KEY(`timea`)
        DISTRIBUTED BY HASH(`timea`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    // non strict insert into select
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
        ("a", "a"),
        (" ", "a"),
        ("12345678908876643", "a"),
        ("1234567890887664643", "b"),
        ("123456789088766445456", "c");
    """
    sql """ insert into datatype_invalid select * from datatype_invalid_base;"""

    qt_select_inserted0 """ select * from datatype_invalid order by timea """

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
    sql """ insert into datatype_invalid select * from datatype_invalid_base;"""

    qt_select_inserted1 """ select * from datatype_invalid order by timea """

    sql """ DROP TABLE IF EXISTS datatype_invalid; """
    sql """
    CREATE TABLE `datatype_invalid` (`timea` datetime NOT NULL, `creatr` varchar(30) NULL)
        UNIQUE KEY(`timea`)
        DISTRIBUTED BY HASH(`timea`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """ insert into datatype_invalid values ('2022-02-29', 'a'); """
    qt_select_inserted2 """ select * from datatype_invalid order by timea """
}
