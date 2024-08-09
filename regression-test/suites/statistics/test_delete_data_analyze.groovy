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

suite("test_delete_data_analyze") {

    sql """drop database if exists test_delete_data_analyze"""
    sql """create database test_delete_data_analyze"""
    sql """use test_delete_data_analyze"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    // Test delete
    sql """CREATE TABLE delete_test (
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
    streamLoad {
        table "delete_test"
        db "test_delete_data_analyze"
        set 'column_separator', '|'
        set 'columns', 'key1, value1, value2'
        set 'strict_mode', 'true'
        file 'load'
        time 10000
    }
    sql """analyze table delete_test with sync"""
    def result = sql """show column stats delete_test"""
    assertEquals(3, result.size())
    assertEquals("5.0", result[0][2])
    assertEquals("5.0", result[1][2])
    assertEquals("5.0", result[2][2])
    sql """delete from delete_test where key1>2"""
    sql """analyze table delete_test with sync"""
    result = sql """show column stats delete_test"""
    assertEquals(3, result.size())
    assertEquals("3.0", result[0][2])
    assertEquals("3.0", result[1][2])
    assertEquals("3.0", result[2][2])

    // Test batch delete
    sql """CREATE TABLE batch_delete_test (
            key1 int NOT NULL,
            value1 varchar(25) NOT NULL,
            value2 varchar(125) NOT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`key1`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """analyze table batch_delete_test with sync"""
    streamLoad {
        table "batch_delete_test"
        db "test_delete_data_analyze"
        set 'column_separator', '|'
        set 'columns', 'key1, value1, value2'
        set 'strict_mode', 'true'
        file 'load'
        time 10000
    }
    sql """analyze table batch_delete_test with sync"""
    result = sql """show column stats batch_delete_test"""
    assertEquals(3, result.size())
    assertEquals("5.0", result[0][2])
    assertEquals("5.0", result[1][2])
    assertEquals("5.0", result[2][2])
    result = sql """show table stats batch_delete_test"""
    assertEquals("5", result[0][0])

    streamLoad {
        table "batch_delete_test"
        db "test_delete_data_analyze"
        set 'column_separator', '|'
        set 'columns', 'key1, value1, value2'
        set 'strict_mode', 'true'
        set 'merge_type', 'DELETE'
        file 'delete'
        time 10000
    }
    result = sql """show table stats batch_delete_test"""
    assertEquals("7", result[0][0])

    streamLoad {
        table "batch_delete_test"
        db "test_delete_data_analyze"
        set 'column_separator', '|'
        set 'columns', 'key1, value1, value2'
        set 'strict_mode', 'true'
        set 'merge_type', 'DELETE'
        file 'load'
        time 10000
    }
    result = sql """show table stats batch_delete_test"""
    assertEquals("12", result[0][0])

    // Sample analyze may get error result, because the reported row count is incorrect before compaction
    //    sql """analyze table batch_delete_test properties("use.auto.analyzer"="true");"""
    sql """analyze table batch_delete_test with sync;"""
    result = sql """show column stats batch_delete_test"""
    assertEquals(3, result.size())
    assertEquals("0.0", result[0][2])
    assertEquals("0.0", result[1][2])
    assertEquals("0.0", result[2][2])


    sql """drop database if exists test_delete_data_analyze"""
}

