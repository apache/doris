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

suite("test_auto_analyze_black_white_list") {

    sql """drop database if exists test_auto_analyze_black_white_list"""
    sql """create database test_auto_analyze_black_white_list"""
    sql """use test_auto_analyze_black_white_list"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE test_bw (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // Test show index row count
    def result = sql """show table stats test_bw"""
    assertEquals(1, result.size())
    assertEquals("true", result[0][8])

    sql """ALTER TABLE test_bw SET ("auto_analyze_policy" = "disable");"""
    result = sql """show table stats test_bw"""
    assertEquals(1, result.size())
    assertEquals("false", result[0][8])

    sql """analyze table test_bw PROPERTIES("use.auto.analyzer"="true")"""
    result = sql """show auto analyze test_bw"""
    assertEquals(0, result.size())

    sql """ALTER TABLE test_bw SET ("auto_analyze_policy" = "enable");"""
    result = sql """show table stats test_bw"""
    assertEquals(1, result.size())
    assertEquals("true", result[0][8])

    sql """analyze table test_bw PROPERTIES("use.auto.analyzer"="true")"""
    result = sql """show auto analyze test_bw"""
    assertEquals(1, result.size())

    sql """ALTER TABLE test_bw SET ("auto_analyze_policy" = "disable");"""
    result = sql """show table stats test_bw"""
    assertEquals(1, result.size())
    assertEquals("false", result[0][8])
    sql """ALTER TABLE test_bw SET ("auto_analyze_policy" = "base_on_catalog");"""
    result = sql """show table stats test_bw"""
    assertEquals(1, result.size())
    assertEquals("true", result[0][8])

    sql """drop database if exists test_auto_analyze_black_white_list"""
}

