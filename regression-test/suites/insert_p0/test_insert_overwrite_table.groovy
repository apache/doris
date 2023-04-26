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

suite("test_iot") {
   def dbname = "test_iot_db";
    sql """drop database if exists ${dbname}"""
    sql """create database ${dbname}"""
    sql """use ${dbname}"""
    sql """clean label from ${dbname}"""

    try {
        sql """
    CREATE TABLE IF NOT EXISTS test_iot (
      `test_datetime` datetime NULL,
      `test_varchar` varchar(150) NULL,
      `test_text` text NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`test_datetime`)
    PARTITION BY RANGE (`test_datetime`)
   (
    PARTITION p1 VALUES [('2023-04-20 00:00:00'),('2023-04-21 00:00:00')),
    PARTITION p2 VALUES [('2023-04-24 00:00:00'),('2023-04-25 00:00:00'))
   )
    DISTRIBUTED BY HASH(`test_datetime`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

       sql """ 
    CREATE TABLE IF NOT EXISTS test_iot1 (
      `test_datetime` datetime NULL,
      `test_varchar` varchar(150) NULL,
      `test_text` text NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`test_datetime`)
    DISTRIBUTED BY HASH(`test_datetime`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

        sql """ INSERT INTO test_iot1(test_datetime, test_varchar, test_text) VALUES ('2023-04-20 16:00:00','aaa','aaa'),('2023-04-24 16:00:00','ccc','ccc') """
        sql """INSERT OVERWRITE TABLE test_iot select * from test_iot1"""
	
        qt_select """select count(*) from test_iot"""

	sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1) select * from test_iot1 where test_varchar = 'aaa' """
	
	qt_select """ select * from test_iot partition p1 """	

	sql """INSERT OVERWRITE TABLE test_iot PARTITION(p1,p2) select * from test_iot1"""

	qt_select """ select * from test_iot partition p1 """

	qt_select """ select * from test_iot partition p2 """		
	
    } finally {
        sql """ DROP TABLE IF EXISTS test_iot """

        sql """ DROP TABLE IF EXISTS test_iot1 """
    }

}
