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

suite("test_array_insert_into_select", "load") {
    // create table with string
    sql "DROP TABLE IF EXISTS tstring"
    sql """
        CREATE TABLE `tstring` (
            `id` INT,
            `data` STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");
    """

    // insert invalid row 0
    sql """INSERT INTO tstring VALUES (0, '[{"a":"1","b":"2","c":"3",}, {"a":"1","b":"2"}]')"""
    // insert valid row 1
    sql """INSERT INTO tstring VALUES (1, '[\\'{"a":"1","b":"2","c":"3",}\\', \\'{"a":"1","b":"2"}\\']')"""
    // insert invalid row 2
    sql """INSERT INTO tstring VALUES (2, '[{"a":"1","b":"2"}]')"""
    // insert valid row 3
    sql """INSERT INTO tstring VALUES (3, '[]')"""

    // check data in tstring
    qt_select_string "SELECT * FROM tstring ORDER BY id"
    qt_select_string "SELECT count(data) FROM tstring"


    // create table with array
    sql "DROP TABLE IF EXISTS tarray"
    sql """
        CREATE TABLE `tarray` (
            `id` INT,
            `data` ARRAY<STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");
    """

    // insert rows to tarray
    sql "INSERT INTO tarray SELECT * FROM tstring"

    // check data in tarray
    qt_select_array "SELECT * FROM tarray ORDER BY id"
    qt_select_array "SELECT count(data) FROM tarray"
}
