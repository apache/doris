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

suite("test_utf_encoding") {

    sql """drop database if exists test_utf_encoding"""
    sql """create database test_utf_encoding"""
    sql """use test_utf_encoding"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE t1 (
            col1 int NOT NULL,
            col2 string NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`col1`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`col1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """insert into t1 values (1, "一般风险准备"), (2, "预计负债")"""
    sql """analyze table t1 with sync"""
    def result =  sql """show column stats t1(col2)"""
    assertEquals("\'一般风险准备\'", result[0][7])
    assertEquals("\'预计负债\'", result[0][8])
    result =  sql"""show column cached stats t1(col2)"""
    assertEquals("\'一般风险准备\'", result[0][7])
    assertEquals("\'预计负债\'", result[0][8])
    explain {
        sql """memo plan select * from t1;"""
        contains("min=64379158486625512.000000(一般风险准备)")
        contains("max=65762361296724456.000000(预计负债)")
    }

    sql """drop database if exists test_utf_encoding"""
}

