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

suite("test_cortas") {
    def dbname = "test_cortas";
    sql """drop database if exists ${dbname}"""
    sql """create database ${dbname}"""
    sql """use ${dbname}"""
    sql """clean label from ${dbname}"""

    def sourceTable = "test_cortas"
    def cortasTable = "test_cortas_cor"

    try {
        sql """
    CREATE TABLE IF NOT EXISTS `${sourceTable}` (
      `test_varchar` varchar(150) NULL,
      `test_text` text NULL,
      `test_datetime` datetime NULL,
      `test_decimalv3` DECIMALV3(7,6) NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

        sql """ INSERT INTO ${sourceTable}(test_varchar, test_text, test_decimalv3, test_datetime) VALUES ('test1','test11',1.1,'2022-04-27 16:00:33'),('test2','test22',2.2,'2022-04-27 16:00:54') """

        sql """
    CREATE OR REPLACE TABLE `${cortasTable}`
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select * from ${sourceTable} order by test_text;
    """

        def res = sql """SHOW CREATE TABLE `${cortasTable}`"""
        assertTrue(res.size() != 0)

        qt_select """select test_varchar, test_text, test_decimalv3, test_datetime from ${cortasTable}"""

        sql """
    CREATE OR REPLACE TABLE `${cortasTable}`
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select test_varchar, lpad(test_text,10,'0') as test_text, test_datetime, test_decimalv3 from ${sourceTable} where test_varchar = "test1";
    """

        res = sql """SHOW CREATE TABLE `${cortasTable}`"""
        assertTrue(res.size() != 0)

        qt_select """select test_varchar, test_text, test_decimalv3, test_datetime, json_object('title', 'decimalv3', 'value', test_decimalv3) from ${cortasTable}"""

    } finally {
        sql """ DROP TABLE IF EXISTS ${sourceTable} """

        sql """ DROP TABLE IF EXISTS ${cortasTable} """

        sql """ DROP DATABASE IF EXISTS ${dbname} """
    }

}

