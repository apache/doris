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

suite("backend_tablets", "p0, external_table,information_schema,backend_tablets") {
    if (!isCloudMode()) {
        def dbName = "test_backend_tablets_db"
        def tbName1 = "test_backend_tablets_1"
        def tbName2 = "test_backend_tablets_2"
        def tbName3 = "test_backend_tablets_3"
        sql(" drop database IF EXISTS ${dbName}")
        sql(" create database ${dbName}")

        sql("use ${dbName}")
        sql("drop table IF EXISTS ${tbName1}")
        sql("drop table IF EXISTS ${tbName2}")
        sql("drop table IF EXISTS ${tbName3}")

        sql """
            CREATE TABLE IF NOT EXISTS `${tbName1}` (
                `aaa` varchar(170) NOT NULL COMMENT "",
                `bbb` varchar(20) NOT NULL COMMENT "",
                `ccc` INT NULL COMMENT "",
                `ddd` SMALLINT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            CREATE TABLE IF NOT EXISTS `${tbName2}` (
                `aaa` varchar(170) NOT NULL COMMENT "",
                `bbb` string NOT NULL COMMENT "",
                `ccc` INT NULL COMMENT "",
                `ddd` SMALLINT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            CREATE TABLE IF NOT EXISTS `${tbName3}` (
                `aaa` varchar(170) NOT NULL COMMENT "",
                `bbb` string NOT NULL COMMENT "",
                `ccc` INT NULL COMMENT "",
                `ddd` BIGINT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            INSERT INTO `${tbName1}` (aaa, bbb, ccc, ddd) VALUES
            ('value1', 'string1', 100, 10),
            ('value2', 'string2', 200, 20),
            ('value3', 'string3', 300, 30),
            ('value4', 'string4', 400, 40),
            ('value5', 'string5', 500, 50),
            ('value6', 'string6', 600, 60),
            ('value7', 'string7', 700, 70),
            ('value8', 'string8', 800, 80),
            ('value9', 'string9', 900, 90),
            ('value10', 'string10', 1000, 100);    
        """

        sql """
            INSERT INTO `${tbName2}` (aaa, bbb, ccc, ddd) VALUES
            ('value1', 'string1', 100, 10),
            ('value2', 'string2', 200, 20),
            ('value3', 'string3', 300, 30),
            ('value4', 'string4', 400, 40),
            ('value5', 'string5', 500, 50),
            ('value6', 'string6', 600, 60),
            ('value7', 'string7', 700, 70),
            ('value8', 'string8', 800, 80),
            ('value9', 'string9', 900, 90),
            ('value10', 'string10', 1000, 100);    
        """

        sql """
            INSERT INTO `${tbName3}` (aaa, bbb, ccc, ddd) VALUES
            ('value1', 'string1', 100, 10),
            ('value2', 'string2', 200, 20),
            ('value3', 'string3', 300, 30),
            ('value4', 'string4', 400, 40),
            ('value5', 'string5', 500, 50),
            ('value6', 'string6', 600, 60),
            ('value7', 'string7', 700, 70),
            ('value8', 'string8', 800, 80),
            ('value9', 'string9', 900, 90),
            ('value10', 'string10', 1000, 100);    
        """

        sql("use information_schema")
        order_qt_1 """ 
            SELECT 
                CASE 
                    WHEN tablets_count.count_result >= 3 THEN 'true'
                    ELSE 'false'
                END AS result
            FROM 
                (SELECT COUNT(*) AS count_result FROM backend_tablets) AS tablets_count;     
        """

    }
}