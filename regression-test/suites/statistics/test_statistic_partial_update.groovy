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

suite("test_statistic_partial_update", "p0, nonConcurrent") {
    sql """drop database if exists test_statistic_partial_update""";
    sql """create database if not exists test_statistic_partial_update"""
    sql """use test_statistic_partial_update"""
    sql """
        CREATE TABLE mvTestUni (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`key1`, `key2`)
        COMMENT "OLAP" 
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "enable_unique_key_merge_on_write" = false,
            "replication_num" = "1"
        );
    """
    sql """set enable_unique_key_partial_update=false"""
    sql """insert into mvTestUni values (1, 2, 3, 4, 5), (1, 2, 3, 7, 8), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""
    sql """set global enable_unique_key_partial_update=true"""
    try {
        try {
            sql """INSERT INTO internal.__internal_schema.column_statistics VALUES ('132440--1-supplier_info',0,11833,132440,-1,'supplier_info',null,144620,1,144620,'null','null',578480,'2024-04-01 09:49:04')"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("You must explicitly specify the columns to be updated when updating partial columns using the INSERT statement"));
        }
        sql """analyze table mvTestUni with sync"""
    } finally {
        sql """set global enable_unique_key_partial_update=false"""
    }
    def result = sql """show column stats mvTestUni"""
    logger.info("result: " + result)
    assertEquals(5, result.size())
    sql """drop database if exists test_statistic_partial_update""";
}

