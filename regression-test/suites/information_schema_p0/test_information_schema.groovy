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

suite("test_information_schema") {
    sql """drop database if exists information_schema_p0"""
    sql """create database information_schema_p0"""
    sql """use information_schema_p0"""
    sql """
        CREATE TABLE IF NOT EXISTS table1  (
            col1 integer not null
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE IF NOT EXISTS table2  (
            col1 integer not null
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE IF NOT EXISTS order1  (
            col1 integer not null
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE IF NOT EXISTS Order2  (
            col1 integer not null
        )
        DUPLICATE KEY(col1)
        DISTRIBUTED BY HASH(col1) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """

    qt_test1 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME = "table1" order by TABLE_NAME;"""
    qt_test2 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME != "table1" order by TABLE_NAME;"""
    qt_test3 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME = "table%" order by TABLE_NAME;"""
    qt_test4 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME != "table%" order by TABLE_NAME;"""
    qt_test5 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME = "order1" order by TABLE_NAME;"""
    qt_test6 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME = "order2" order by TABLE_NAME;"""
    qt_test7 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME = "Order2" order by TABLE_NAME;"""
    qt_test8 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME like "table%" order by TABLE_NAME;"""
    qt_test9 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME not like "table%" order by TABLE_NAME;"""
    qt_test10 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME like "order%" order by TABLE_NAME;"""
    qt_test11 """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "information_schema_p0" and TABLE_NAME like "Order%" order by TABLE_NAME;"""
}
