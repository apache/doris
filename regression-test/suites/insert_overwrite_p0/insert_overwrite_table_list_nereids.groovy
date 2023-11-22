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

suite("test_iot_list_nereids") {
    sql """set enable_nereids_planner = true"""
    sql """set enable_fallback_to_original_planner = false"""
    sql """set enable_nereids_dml = true"""
    def dbName = "test_iot_db_list_nereids";
    sql """drop database if exists ${dbName}"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    try {
    sql """
    CREATE TABLE IF NOT EXISTS test_iot (
      `test_int` int NOT NULL,
      `test_varchar` varchar(150) NULL,
      `test_text` text NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`test_int`)
    PARTITION BY LIST (`test_int`)
   (
    PARTITION p1 VALUES IN ("1","2","3"),
    PARTITION p2 VALUES IN ("4","5","6")
   )
    DISTRIBUTED BY HASH(`test_int`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

    sql """
    CREATE TABLE IF NOT EXISTS test_iot1 (
      `test_int` int NOT NULL,
      `test_varchar` varchar(150) NULL,
      `test_text` text NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`test_int`)
    DISTRIBUTED BY HASH(`test_int`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """
    sql """
    CREATE TABLE IF NOT EXISTS test_iot_err (
      `test_varchar1` varchar(150) NULL,
      `test_varchar2` varchar(150) NULL,
      `test_text` text NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar1`)
    DISTRIBUTED BY HASH(`test_varchar1`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """
       // overwrite not existed table
       try {
           sql """INSERT OVERWRITE TABLE test_iot_wrong select * from test_iot1"""
           throw new IllegalStateException("Should be not existed table")
        } catch (java.sql.SQLException t) {
           assertTrue(true)
       } 
       
        // overwrite with unqualified data
       sql """ INSERT INTO test_iot_err(test_varchar1, test_varchar2, test_text) VALUES ('xxx','aaa','aaa'),('xxx','ccc','ccc') """
        try {
           sql """INSERT OVERWRITE TABLE test_iot select * from test_iot_err"""
           throw new IllegalStateException("Should be wrong data")
        } catch (java.sql.SQLException t) {
           assertTrue(true)
       }
        // overwrite with empty data 
        sql """INSERT OVERWRITE TABLE test_iot select * from test_iot1"""
        order_qt_select """select * from test_iot"""
        
        // overwrite with values list
        sql """INSERT OVERWRITE TABLE test_iot VALUES (1,'aaa','aaa'),(4,'ccc','ccc')"""   
        order_qt_select """select * from test_iot"""
     
        // overwrite with origin data
        sql """INSERT OVERWRITE TABLE test_iot select 1,'aaa','aaa' """
        order_qt_select """select * from test_iot"""
       
        // overwrite with another table
        sql """ INSERT INTO test_iot1(test_int, test_varchar, test_text) VALUES (1,'aaa','aaa'),(4,'ccc','ccc') """
        sql """INSERT OVERWRITE TABLE test_iot select * from test_iot1"""
        order_qt_select """select * from test_iot"""
       
        // overwrite table with some cols
        sql """INSERT OVERWRITE TABLE test_iot (test_int, test_varchar) select test_int, test_varchar from test_iot1"""
        order_qt_select """select * from test_iot"""

       // overwrite with label
       sql """INSERT OVERWRITE TABLE test_iot WITH LABEL `label1` (test_int, test_varchar, test_text) select * from test_iot1"""
       order_qt_select """select * from test_iot"""
       
       // overwrite not existed partition
        try {
           sql """INSERT OVERWRITE TABLE test_iot PARTITION(p3) select * from test_iot1"""
           throw new IllegalStateException("Should be not existed partition")
        } catch (java.sql.SQLException t) {
           assertTrue(true)
       }

       // overwrite with data which is not in partition's list
        try {
           sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) select * from test_iot1 where test_int = 4 """
           throw new IllegalStateException("Should be wrong data")
        } catch (java.sql.SQLException t) {
           assertTrue(true)
       }
        // overwrite partition with values list
        sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) VALUES (1,'aaa','aaa')"""   
        order_qt_select """select * from test_iot"""
     
        // overwrite partition with origin data
        sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) select 1,'aaa','aaa' """
        order_qt_select """select * from test_iot"""

        // overwrite partition with another table
        sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) select * from test_iot1 where test_varchar = 'aaa' """
        order_qt_select """ select * from test_iot partition p1 """

        // overwrite tow partitions with another table
        sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1,p2) select * from test_iot1"""
        order_qt_select """ select * from test_iot partition p1 """
        order_qt_select """ select * from test_iot partition p2 """

        // overwrite partition with some cols
        sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) (test_int, test_varchar) select test_int, test_varchar from test_iot1 where test_varchar = 'aaa'"""
        order_qt_select """select * from test_iot"""

       // overwrite partition with label
       sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) WITH LABEL `label2` (test_int, test_varchar, test_text) select * from test_iot1 where test_varchar = 'aaa'"""
       order_qt_select """select * from test_iot"""

    } finally {
        sql """ DROP TABLE IF EXISTS test_iot """

        sql """ DROP TABLE IF EXISTS test_iot1 """

        sql """ DROP DATABASE IF EXISTS ${dbName} """
    }

}

