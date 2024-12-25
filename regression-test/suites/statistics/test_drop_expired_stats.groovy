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

suite("test_drop_expired_stats") {

    sql """drop database if exists test_drop_expired_stats"""
    sql """create database test_drop_expired_stats"""
    sql """use test_drop_expired_stats"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE table1 (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """CREATE TABLE table2 (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    def id1 = getTableId("test_drop_expired_stats", "table1")
    def id2 = getTableId("test_drop_expired_stats", "table2")

    sql """analyze table table1 with sync"""
    sql """analyze table table2 with sync"""
    def result = sql """select * from __internal_schema.column_statistics where tbl_id = ${id1}"""
    assertEquals(5, result.size())
    result = sql """select * from __internal_schema.column_statistics where tbl_id = ${id2}"""
    assertEquals(3, result.size())
    sql """drop table table1"""
    sql """drop expired stats"""
    result = sql """select * from __internal_schema.column_statistics where tbl_id = ${id1}"""
    assertEquals(0, result.size())
    result = sql """select * from __internal_schema.column_statistics where tbl_id = ${id2}"""
    assertEquals(3, result.size())

    sql """drop database if exists test_drop_expired_stats"""
    sql """drop expired stats"""
    result = sql """select * from __internal_schema.column_statistics where tbl_id = ${id1}"""
    assertEquals(0, result.size())
    result = sql """select * from __internal_schema.column_statistics where tbl_id = ${id2}"""
    assertEquals(0, result.size())
}

