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

suite("test_round") {
    // vectorized
    sql """ set enable_vectorized_engine = true """

    qt_select "SELECT round(10.12345)"
    qt_select "SELECT round(10.12345, 2)"

    def tableName = "test_round"
    sql """DROP TABLE IF EXISTS `${tableName}`"""
    sql """ CREATE TABLE `${tableName}` (
        `col1` DECIMALV3(6,3) COMMENT "",
        `col2` DECIMALV3(16,5) COMMENT "",
        `col3` DECIMALV3(32,5) COMMENT "")
        DUPLICATE KEY(`col1`) DISTRIBUTED BY HASH(`col1`)
        PROPERTIES ( "replication_num" = "1" ); """

    sql """ insert into `${tableName}` values(16.025, 16.025, 16.025); """
    qt_select """ SELECT round(col1, 2), round(col2, 2), round(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1, 2), floor(col2, 2), floor(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1, 2), ceil(col2, 2), ceil(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT truncate(col1, 2), truncate(col2, 2), truncate(col3, 2) FROM `${tableName}`; """
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
