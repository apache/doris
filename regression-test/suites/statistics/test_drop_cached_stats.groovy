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

suite("test_drop_cached_stats") {

    sql """drop database if exists test_drop_cached_stats"""
    sql """create database test_drop_cached_stats"""
    sql """use test_drop_cached_stats"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE drop_cache_test (
            key1 int NOT NULL,
            value1 varchar(25) NOT NULL,
            value2 varchar(125) NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    createMV("create materialized view mv1 as select key1 from drop_cache_test;")

    sql """insert into drop_cache_test values (1, "1", "1")"""
    sql """analyze table drop_cache_test with sync"""

    def result = sql """show column stats drop_cache_test"""
    assertEquals(4, result.size())
    result = sql """show column cached stats drop_cache_test"""
    assertEquals(4, result.size())

    sql """drop cached stats drop_cache_test"""
    result = sql """show column cached stats drop_cache_test"""
    assertEquals(0, result.size())

    result = sql """show column stats drop_cache_test"""
    assertEquals(4, result.size())


    sql """drop database if exists test_drop_cached_stats"""
}

